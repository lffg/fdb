use async_trait::async_trait;
use buff::Buff;

use crate::{
    catalog::{
        object::ObjectType,
        page::{HeapPage, PageId},
    },
    error::DbResult,
    exec::{
        common::{find_object, object_is_not_table},
        serde::deserialize_table_record,
        value::Environment,
        ExecCtx, Executor,
    },
    io::pager::Pager,
};

/// An select command.
pub struct Select<'a> {
    table_name: &'a str,
    state: Option<IterState>,
}

/// Iterator state.
struct IterState {
    page: PageId,
    rem_total: u64,
    rem_page: u16,
    offset: u16,
}

impl IterState {
    async fn init(pager: &Pager, first_page_id: PageId) -> DbResult<Self> {
        let guard = pager.get::<HeapPage>(first_page_id).await?;
        let page = guard.read().await;

        let seq_header = page.header.seq_header.as_ref().expect("first page");
        let rem_total = seq_header.record_count;
        let rem_page = page.header.record_count;

        page.release();

        Ok(Self {
            page: first_page_id,
            rem_total,
            rem_page,
            offset: 0,
        })
    }
}

#[async_trait]
impl Executor for Select<'_> {
    type Item<'a> = Option<Environment>;

    async fn next<'a>(&mut self, ctx: &'a ExecCtx) -> DbResult<Option<Self::Item<'a>>> {
        let object = find_object(ctx, self.table_name)?;
        let ObjectType::Table(table) = object.ty else {
            return Err(object_is_not_table(&object));
        };
        // Set first state.
        let state = if let Some(state) = &mut self.state {
            state
        } else {
            self.state
                .insert(IterState::init(ctx.pager, object.page_id).await?)
        };

        if state.rem_total == 0 {
            return Ok(None);
        }

        let guard = ctx.pager.get::<HeapPage>(state.page).await?;
        let page = guard.read().await;

        if state.rem_page == 0 {
            let Some(next_page) = page.header.next_page_id else {
                return Ok(None);
            };
            // Load next page.
            let guard = ctx.pager.get::<HeapPage>(next_page).await?;
            let page = guard.read().await;
            state.rem_page = page.header.record_count;
            state.offset = 0;
            state.page = next_page;
            page.release();
        }

        // TODO: HACK: One must be able to create a buf from a shared slice.
        let mut cloned_buf = page.bytes[state.offset as usize..].to_owned();
        let mut buf = Buff::new(&mut cloned_buf);

        page.release();

        let (delta, result) = buf.delta(|buf| deserialize_table_record(buf, &table));
        state.offset += delta as u16;

        state.rem_total -= 1;
        state.rem_page -= 1;

        Ok(Some(result?))
    }
}

impl<'s> Select<'s> {
    /// Creates a new insert executor.
    pub fn new(table_name: &'s str) -> Select<'s> {
        Self {
            table_name,
            state: None,
        }
    }
}
