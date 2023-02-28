use buff::Buff;

use crate::{
    catalog::{
        object::ObjectType,
        page::{FirstHeapPage, OrdinaryHeapPage, PageId},
    },
    error::DbResult,
    exec::{
        common::{find_object, object_is_not_table},
        serde::deserialize_table_record,
        value::Environment,
        ExecCtx, Executor,
    },
    pager::Pager,
};

/// An select command.
pub struct Select<'a> {
    table_name: &'a str,
    state: Option<IterState>,
}

/// Iterator state.
struct IterState {
    /// Current page.
    bytes: Vec<u8>, // This will change soon.
    next_page: Option<PageId>,
    rem_total: u64,
    rem_page: u16,
    next_offset: u16,
}

impl IterState {
    fn init(pager: &mut Pager, first_page_id: PageId) -> DbResult<Self> {
        let page: FirstHeapPage = pager.load(first_page_id)?;
        Ok(Self {
            bytes: page.ordinary_page.bytes,
            next_page: page.ordinary_page.next_page_id,
            rem_total: page.total_record_count,
            rem_page: page.ordinary_page.record_count,
            next_offset: 0,
        })
    }
}

impl Executor for Select<'_> {
    type Item<'a> = Option<Environment>;

    fn next<'a>(&mut self, ctx: &'a mut ExecCtx) -> DbResult<Option<Self::Item<'a>>> {
        let object = find_object(ctx, &self.table_name)?;
        let ObjectType::Table(table) = object.ty else {
            return Err(object_is_not_table(&object));
        };
        // Set first state.
        let state = if let Some(state) = &mut self.state {
            state
        } else {
            self.state
                .insert(IterState::init(ctx.pager, object.page_id)?)
        };

        if state.rem_total == 0 {
            return Ok(None);
        }
        if state.rem_page == 0 {
            let Some(next_page) = state.next_page else {
                return Ok(None);
            };
            // Load next page.
            let page: OrdinaryHeapPage = ctx.pager.load(next_page)?;
            state.bytes = page.bytes;
            state.next_page = page.next_page_id;
            state.rem_page = page.record_count;
            state.next_offset = 0;
        }

        state.rem_total -= 1;
        state.rem_page -= 1;

        let mut buf = Buff::new(&mut state.bytes[state.next_offset as usize..]);

        let (delta, result) = buf.delta(|buf| deserialize_table_record(buf, &table));
        state.next_offset += delta as u16;

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
