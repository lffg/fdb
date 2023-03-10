use async_trait::async_trait;
use tracing::{instrument, trace};

use crate::{
    catalog::{
        object::Object,
        page::{HeapPage, PageId},
        record::simple_record::{self, SimpleRecord},
    },
    error::DbResult,
    exec::query::{Executor, QueryCtx},
    io::pager::PagerGuard,
    util::io::{SerdeCtx, Size},
};

// TODO: Dup.
const FIRST_SCHEMA_PAGE_ID: PageId = PageId::new_u32(2);

/// An object selection operation.
#[derive(Default)]
pub struct ObjectSelect {
    state: Option<State>,
}

struct State {
    page_id: PageId,
    rem_total: u64,
    rem_page: u16,
    offset: u16,
}

#[async_trait]
impl Executor for ObjectSelect {
    type Item<'a> = Object;

    #[instrument(skip_all)]
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

            let serde_ctx = simple_record::OffsetCtx {
                offset: state.offset,
            };

            let record = page.read_at(state.offset, |buf| {
                SimpleRecord::<Object>::deserialize(buf, serde_ctx)
            })?;

            state.offset += record.size() as u16;
            state.rem_total -= 1;
            state.rem_page -= 1;

            if record.is_deleted() {
                continue;
            }

            return Ok(Some(record.into_data().into_owned()));
        }
    }
}

impl ObjectSelect {
    pub fn new() -> ObjectSelect {
        Self { state: None }
    }

    /// Initializes the state.
    async fn get_or_init_state(
        &mut self,
        ctx: &QueryCtx<'_>,
    ) -> DbResult<(PagerGuard<HeapPage>, &mut State)> {
        match &mut self.state {
            Some(state) => Ok((ctx.pager.get::<HeapPage>(state.page_id).await?, state)),
            state @ None => {
                trace!(page = ?FIRST_SCHEMA_PAGE_ID, "reading first page from sequence");
                let guard = ctx.pager.get::<HeapPage>(FIRST_SCHEMA_PAGE_ID).await?;
                let page = guard.read().await;

                let state = state.insert(State {
                    page_id: FIRST_SCHEMA_PAGE_ID,
                    rem_total: page // TODO: Dup with macro foo!(mut|ref, ...).
                        .header
                        .seq_header
                        .as_ref()
                        .expect("first page")
                        .record_count,
                    rem_page: page.header.record_count,
                    offset: page.first_offset(),
                });

                page.release();
                Ok((guard, state))
            }
        }
    }
}