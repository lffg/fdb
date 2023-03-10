use async_trait::async_trait;
use tracing::trace;

use crate::{
    catalog::{
        object::Object,
        page::{HeapPage, PageId},
        record::simple_record::{self, SimpleRecord},
        table_schema::TableSchema,
    },
    error::DbResult,
    exec::{
        query::{Query, QueryCtx},
        values::SchematizedValues,
    },
    io::pager::PagerGuard,
    util::io::{SerdeCtx, Size},
};

/// A linear scan query.
pub struct LinearScan<'a> {
    table_name: &'a str,
    state: Option<State>,
}

struct State {
    table_schema: TableSchema,
    page_id: PageId,
    rem_total: u64,
    rem_page: u16,
    offset: u16,
}

#[async_trait]
impl Query for LinearScan<'_> {
    type Item<'a> = SimpleRecord<'static, SchematizedValues<'static>>;

    async fn next<'a>(&mut self, ctx: &'a QueryCtx<'a>) -> DbResult<Option<Self::Item<'a>>> {
        loop {
            let (page_guard, state) = self.get_or_init_state(ctx).await?;
            let page = page_guard.read().await;

            if state.rem_total == 0 {
                page.release();
                return Ok(None);
            }

            if state.rem_page == 0 {
                state.page_id = page
                    .header
                    .next_page_id
                    .expect("bug: counters aren't synchronized");
                page.release();
                trace!("moving to next page in the sequence");
                continue;
            }

            let serde_ctx = simple_record::TableRecordCtx {
                schema: &state.table_schema,
                offset: state.offset,
            };

            let record = page.read_at(state.offset, |buf| {
                SimpleRecord::<SchematizedValues>::deserialize(buf, serde_ctx)
            })?;

            state.offset += record.size() as u16;
            state.rem_total -= 1;
            state.rem_page -= 1;

            return Ok(Some(record));
        }
    }
}

impl<'s> LinearScan<'s> {
    /// Creates a new insert executor.
    pub fn new(table_name: &'s str) -> LinearScan<'s> {
        Self {
            table_name,
            state: None,
        }
    }

    async fn get_or_init_state(
        &mut self,
        ctx: &QueryCtx<'_>,
    ) -> DbResult<(PagerGuard<HeapPage>, &mut State)> {
        match &mut self.state {
            Some(state) => Ok((ctx.pager.get::<HeapPage>(state.page_id).await?, state)),
            state @ None => {
                trace!("fetching table schema");
                let table_object = Object::find(ctx, self.table_name).await?;
                let first_page_id = table_object.page_id;
                let table_schema = table_object.try_into_table_schema()?;

                trace!("loading first page of sequence");
                let guard = ctx.pager.get::<HeapPage>(first_page_id).await?;
                let page = guard.read().await;

                let seq_header = page.header.seq_header.as_ref().expect("first page");
                let rem_total = seq_header.record_count;
                let rem_page = page.header.record_count;

                page.release();

                Ok((
                    guard,
                    state.insert(State {
                        table_schema,
                        page_id: first_page_id,
                        rem_total,
                        rem_page,
                        offset: 0,
                    }),
                ))
            }
        }
    }
}
