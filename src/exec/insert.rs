use async_trait::async_trait;
use buff::Buff;

use crate::{
    catalog::{object::ObjectType, page::HeapPage},
    error::DbResult,
    exec::{
        common::{find_object, object_is_not_table},
        serde::serialize_table_record,
        value::Environment,
        ExecCtx, Executor,
    },
};

/// An insert operation.
pub struct Insert<'s> {
    /// The table name.
    table_name: &'s str,
    /// The values to be inserted.
    env: Environment,
}

#[async_trait]
impl Executor for Insert<'_> {
    // TODO: Add number of inserted rows.
    type Item<'a> = ();

    async fn next<'a>(&mut self, ctx: &'a mut ExecCtx) -> DbResult<Option<Self::Item<'a>>> {
        let object = find_object(ctx, self.table_name)?;
        let ObjectType::Table(table) = object.ty else {
            return Err(object_is_not_table(&object));
        };

        let mut page: HeapPage = ctx.pager.load(object.page_id).await?;
        let seq_header = page.seq_header.as_mut().expect("first page");

        if seq_header.last_page_id != object.page_id {
            todo!("implement me");
        }
        // TODO: Handle not enough bytes left to store in the current page.
        let _needed = self.env.size();

        // Insert the record.
        let mut buf = Buff::new(&mut page.bytes[page.free_offset as usize..]);
        let (delta, result) = buf.delta(|buf| serialize_table_record(buf, &table, &self.env));
        result?;

        // Update metadata.
        seq_header.record_count += 1;
        page.record_count += 1;
        page.free_offset += u16::try_from(delta).unwrap();

        ctx.pager.write_flush(&page).await?;

        Ok(None)
    }
}

impl<'s> Insert<'s> {
    /// Creates a new insert executor.
    pub fn new(table_name: &'s str, env: Environment) -> Insert<'s> {
        Self { table_name, env }
    }
}
