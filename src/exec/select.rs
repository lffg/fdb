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
    pager::Pager,
};

/// An select command.
pub struct Select<'a> {
    table_name: &'a str,
    state: Option<IterState>,
}

/// Iterator state.
struct IterState {
    page: Box<HeapPage>,
    rem_total: u64,
    rem_page: u16,
    offset: u16,
}

impl IterState {
    fn init(pager: &mut Pager, first_page_id: PageId) -> DbResult<Self> {
        let page: HeapPage = pager.load(first_page_id)?;
        let seq_header = page.seq_header.as_ref().expect("first page");
        Ok(Self {
            rem_total: seq_header.record_count,
            rem_page: page.record_count,
            page: Box::new(page),
            offset: 0,
        })
    }
}

impl Executor for Select<'_> {
    type Item<'a> = Option<Environment>;

    fn next<'a>(&mut self, ctx: &'a mut ExecCtx) -> DbResult<Option<Self::Item<'a>>> {
        let object = find_object(ctx, self.table_name)?;
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
            let Some(next_page) = state.page.next_page_id else {
                return Ok(None);
            };
            // Load next page.
            let page: HeapPage = ctx.pager.load(next_page)?;
            state.rem_page = page.record_count;
            state.offset = 0;
            state.page = Box::new(page);
        }

        let mut buf = Buff::new(&mut state.page.bytes[state.offset as usize..]);

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
