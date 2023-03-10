use async_trait::async_trait;
use tracing::{debug, instrument};

use crate::{
    catalog::{
        object::Object,
        page::{HeapPage, PageId},
        record::simple_record::{self, SimpleRecord},
    },
    error::DbResult,
    exec::query::Query,
    io::pager::PagerGuard,
    util::io::{SerdeCtx, Size},
    Db,
};

// TODO: Dup.
const FIRST_SCHEMA_PAGE_ID: PageId = PageId::new_u32(2);

/// An object selection query.
#[derive(Default)]
pub struct Select {
    state: Option<State>,
}

struct State {
    page_id: PageId,
    rem_total: u64,
    rem_page: u16,
    offset: u16,
}

#[async_trait]
impl Query for Select {
    type Item<'a> = Object;

    #[instrument(name = "ObjectSelect", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        loop {
            let (page_guard, state) = self.get_or_init_state(db).await?;
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

            let serde_ctx = simple_record::SimpleCtx {
                page_id: state.page_id,
                offset: state.offset,
            };

            let record = page.read_at(state.offset, |buf| {
                SimpleRecord::<Object>::deserialize(buf, serde_ctx)
            })?;

            state.offset += record.size() as u16;
            state.rem_total -= 1;
            state.rem_page -= 1;

            page.release();

            if record.is_deleted() {
                continue;
            }

            return Ok(Some(record.into_data().into_owned()));
        }
    }
}

impl Select {
    pub fn new() -> Select {
        Self { state: None }
    }

    /// Initializes the state.
    async fn get_or_init_state(&mut self, db: &Db) -> DbResult<(PagerGuard<HeapPage>, &mut State)> {
        match &mut self.state {
            Some(state) => Ok((db.pager().get::<HeapPage>(state.page_id).await?, state)),
            state @ None => {
                debug!(page = ?FIRST_SCHEMA_PAGE_ID, "reading first page from sequence");
                let guard = db.pager().get::<HeapPage>(FIRST_SCHEMA_PAGE_ID).await?;
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
