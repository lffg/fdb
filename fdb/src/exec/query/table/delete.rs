use async_trait::async_trait;
use tracing::trace;

use crate::{
    catalog::{object::Object, page::HeapPage, record::simple_record, table_schema::TableSchema},
    error::DbResult,
    exec::{
        query::{table::LinearScan, Query, QueryCtx},
        values::Values,
    },
    util::io::SerdeCtx,
};

/// The deletion predicate.
pub type Pred = dyn Sync + for<'v> Fn(&'v Values) -> bool;

/// A delete query.
pub struct Delete<'a> {
    table_name: &'a str,
    table_schema: Option<TableSchema>,
    linear_scan: LinearScan<'a>,
    pred: &'a Pred,
}

#[async_trait]
impl Query for Delete<'_> {
    // TODO: Add `deleted_count`.
    type Item<'a> = ();

    async fn next<'a>(&mut self, ctx: &'a QueryCtx<'a>) -> DbResult<Option<Self::Item<'a>>> {
        loop {
            let out = if let Some(mut record) = self.linear_scan.next(ctx).await? {
                let values = record.as_data().as_values();

                if record.is_deleted() || !(self.pred)(values) {
                    continue;
                }

                let page_id = record.page_id();
                let offset = record.offset();
                trace!(?page_id, "allocating page for write");
                let guard = ctx.pager.get::<HeapPage>(page_id).await?;
                let mut page = guard.write().await;

                // TODO: Remove this. Pass via `Delete` argument.
                let ctx = simple_record::TableRecordCtx {
                    page_id,
                    offset,
                    schema: self.get_or_init_schema(ctx).await?,
                };

                record.set_deleted();
                page.write_at(offset, |buf| record.serialize(buf, ctx))?;

                page.flush();
                Some(())
            } else {
                ctx.pager.flush_all().await?;
                None
            };
            return Ok(out);
        }
    }
}

impl<'s> Delete<'s> {
    pub fn new(table_name: &'s str, pred: &'s Pred) -> Delete<'s> {
        Self {
            table_name,
            table_schema: None,
            linear_scan: LinearScan::new(table_name),
            pred,
        }
    }

    /// Initializes the schema, if needed.
    async fn get_or_init_schema(&mut self, ctx: &QueryCtx<'_>) -> DbResult<&mut TableSchema> {
        Ok(match &mut self.table_schema {
            Some(schema) => schema,
            schema @ None => schema.insert(
                Object::find(ctx, self.table_name)
                    .await?
                    .try_into_table_schema()?,
            ),
        })
    }
}
