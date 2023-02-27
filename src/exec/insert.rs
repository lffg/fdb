use buff::Buff;

use crate::{
    catalog::{
        object::ObjectType,
        page::{FirstHeapPage, FIRST_HEAP_PAGE_HEADER_SIZE},
    },
    error::DbResult,
    exec::{
        common::{find_object, object_is_not_table},
        serde::serialize_table_record,
        value::Environment,
        Command, ExecCtx,
    },
};

/// An insert command.
pub struct InsertCmd<'a> {
    pub table_name: &'a str,
    pub env: Environment,
}

impl Command for InsertCmd<'_> {
    type Ret = ();

    fn execute(self, ctx: &mut ExecCtx) -> DbResult<Self::Ret> {
        let object = find_object(ctx, &self.table_name)?;
        let ObjectType::Table(table) = object.ty else {
            return Err(object_is_not_table(&object));
        };

        let mut first_page: FirstHeapPage = ctx.pager.load(object.page_id)?;

        if first_page.last_page_id != object.page_id {
            todo!("implement me");
        }
        // TODO: Handle not enough bytes left to store in the current page.
        let _needed = self.env.size();
        let _remaining = first_page.ordinary_page.remaining();

        // Insert the record.
        let offset = first_page.ordinary_page.free_offset - FIRST_HEAP_PAGE_HEADER_SIZE;
        let bytes = &mut first_page.ordinary_page.bytes;
        let mut buf = Buff::new(&mut bytes[offset as usize..]);
        let (delta, result) = buf.delta(|buf| serialize_table_record(buf, &table, &self.env));
        result?;

        // Update metadata.
        first_page.total_record_count += 1;
        first_page.ordinary_page.record_count += 1;
        first_page.ordinary_page.free_offset += u16::try_from(delta).unwrap();

        ctx.pager.write_flush(&first_page)?;

        Ok(())
    }
}
