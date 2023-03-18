use async_trait::async_trait;
use tracing::{debug, instrument};

use crate::{
    catalog::{
        object::TableObject,
        page::{HeapPage, PageId},
        record::simple_record::{self, SimpleRecord},
    },
    error::DbResult,
    exec::{query::Query, util::macros::seq_h, values::SchematizedValues},
    io::pager::PagerGuard,
    util::io::{SerdeCtx, Size},
    Db,
};

/// A linear scan query.
pub struct LinearScan<'a> {
    table: &'a TableObject,
    state: Option<State>,
}

#[derive(Debug)]
struct State {
    page_id: PageId,
    rem_total: u64,
    rem_page: u16,
    offset: u16,
}

type Record = SimpleRecord<'static, SchematizedValues<'static>>;

#[async_trait]
impl Query for LinearScan<'_> {
    type Item<'a> = Record;

    #[instrument(name = "TableLinearScan", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        let maybe_record = self.peek(db).await?;

        let Some(record) = maybe_record else {
            return Ok(None);
        };

        // SAFETY: `peek` initializes the state.
        let state = unsafe { self.state.as_mut().unwrap_unchecked() };
        state.offset += record.size() as u16;
        state.rem_total -= 1;
        state.rem_page -= 1;

        Ok(Some(record))
    }
}

impl<'a> LinearScan<'a> {
    /// Creates a new insert executor.
    pub fn new(table: &'a TableObject) -> LinearScan<'a> {
        Self { table, state: None }
    }

    /// Returns the current element without advancing the underlying iterator.
    ///
    /// This method doesn't perform any kind of cache, which is handled by the
    /// underlying database pager.
    pub async fn peek(&mut self, db: &Db) -> DbResult<Option<Record>> {
        loop {
            let (page_guard, state) =
                Self::get_or_init_state(db, &mut self.state, self.table.page_id).await?;
            let page = page_guard.read().await;

            if state.rem_total == 0 {
                page.release();
                return Ok(None);
            }

            if state.rem_page == 0 {
                let next_page_id = page
                    .header
                    .next_page_id
                    .expect("bug: counters aren't synchronized");
                Self::load_next_state_for_page(db, state, next_page_id).await?;
                page.release();
                debug!("moving to next page in the sequence");
                continue;
            }

            let serde_ctx = simple_record::TableRecordCtx {
                page_id: state.page_id,
                offset: state.offset,
                schema: &self.table.schema,
            };

            let record = page.read_at(state.offset, |buf| {
                SimpleRecord::<SchematizedValues>::deserialize(buf, serde_ctx)
            })?;

            page.release();

            return Ok(Some(record));
        }
    }

    async fn get_or_init_state<'s>(
        db: &Db,
        state: &'s mut Option<State>,
        first_page_id: PageId,
    ) -> DbResult<(PagerGuard<HeapPage>, &'s mut State)> {
        match state {
            Some(state) => Ok((db.pager().get::<HeapPage>(state.page_id).await?, state)),
            state @ None => {
                debug!("loading first page of sequence");
                let guard = db.pager().get::<HeapPage>(first_page_id).await?;
                let page = guard.read().await;

                let state = state.insert(State {
                    page_id: first_page_id,
                    rem_total: seq_h!(page).record_count,
                    rem_page: page.header.record_count,
                    offset: page.first_offset(),
                });

                page.release();

                Ok((guard, state))
            }
        }
    }

    async fn load_next_state_for_page(db: &Db, state: &mut State, page_id: PageId) -> DbResult<()> {
        let guard = db.pager().get::<HeapPage>(page_id).await?;
        let page = guard.read().await;

        state.page_id = page_id;
        state.rem_page = page.header.record_count;
        state.offset = page.first_offset();

        page.release();
        Ok(())
    }
}
