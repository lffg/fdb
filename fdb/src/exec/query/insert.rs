use std::borrow::Cow;

use async_trait::async_trait;
use buff::Buff;

use crate::{
    catalog::{
        object::ObjectType,
        page::HeapPage,
        record::simple_record::{self, SimpleRecord},
    },
    error::DbResult,
    exec::{
        object::{find_object, object_is_not_table},
        query::{Executor, QueryCtx},
        values::Values,
    },
    util::io::{SerdeCtx, Size},
};

/// An insert operation.
pub struct Insert<'s> {
    /// The table name.
    table_name: &'s str,
    /// The values to be inserted.
    values: Values,
}

#[async_trait]
impl Executor for Insert<'_> {
    // TODO: Add number of inserted rows.
    type Item<'a> = ();

    // i know, this is horrific. i will refactor this soon.
    async fn next<'a>(&mut self, ctx: &'a QueryCtx<'a>) -> DbResult<Option<Self::Item<'a>>> {
        let object = find_object(ctx, self.table_name)?;
        let ObjectType::Table(table_schema) = object.ty else {
            return Err(object_is_not_table(&object));
        };

        let schematized_values = self.values.schematize(&table_schema)?;

        let seq_first_p_guard = ctx.pager.get::<HeapPage>(object.page_id).await?;
        let mut seq_first_p = seq_first_p_guard.write().await;

        let seq_header = seq_first_p.header.seq_header.as_mut().expect("first page");

        if seq_header.last_page_id != object.page_id {
            todo!("implement me");
        }

        seq_header.record_count += 1;

        let serde_ctx = simple_record::TableRecordCtx {
            schema: &table_schema,
            offset: seq_first_p.header.free_offset,
        };
        let record = SimpleRecord::<crate::exec::values::SchematizedValues>::new(
            serde_ctx.offset,
            Cow::Owned(schematized_values),
        );
        let size = record.size();

        // Insert the record.
        let mut buf = Buff::new(&mut seq_first_p.bytes[serde_ctx.offset as usize..]);
        record.serialize(&mut buf, serde_ctx)?;

        // Update metadata.
        seq_first_p.header.record_count += 1;
        seq_first_p.header.free_offset += u16::try_from(size).unwrap();

        seq_first_p.flush();

        ctx.pager.flush_all().await?;

        Ok(None)
    }
}

impl<'s> Insert<'s> {
    /// Creates a new insert executor.
    pub fn new(table_name: &'s str, values: Values) -> Insert<'s> {
        Self { table_name, values }
    }
}
