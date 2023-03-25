use async_trait::async_trait;
use tracing::{debug, instrument};

use crate::{
    catalog::{object::TableObject, page::HeapPage, record::simple_record},
    error::DbResult,
    exec::{
        query::{table::LinearScan, Query},
        values::Values,
    },
    util::io::SerializeCtx,
    Db,
};

/// The deletion predicate.
pub type Pred = dyn Sync + for<'v> Fn(&'v Values) -> bool;

/// A delete query.
pub struct Delete<'a> {
    table: &'a TableObject,
    linear_scan: LinearScan<'a>,
    pred: &'a Pred,
}

#[async_trait]
impl Query for Delete<'_> {
    // TODO: Add `deleted_count`.
    type Item<'a> = ();

    #[instrument(name = "TableDelete", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        loop {
            let out = if let Some(mut record) = self.linear_scan.next(db).await? {
                let values = record.as_data().as_values();

                if record.is_deleted() || !(self.pred)(values) {
                    continue;
                }

                let page_id = record.page_id();
                let offset = record.offset();
                debug!(?page_id, "allocating page for write");
                let guard = db.pager().get::<HeapPage>(page_id).await?;
                let mut page = guard.write().await;

                let ctx = simple_record::TableRecordCtx {
                    page_id,
                    offset,
                    schema: &self.table.schema,
                };

                record.set_deleted();
                page.write_at(offset, |buf| record.serialize(buf, &ctx))?;

                page.flush();
                Some(())
            } else {
                db.pager().flush_all().await?;
                None
            };
            return Ok(out);
        }
    }
}

impl<'s> Delete<'s> {
    pub fn new(table: &'s TableObject, pred: &'s Pred) -> Delete<'s> {
        Self {
            linear_scan: LinearScan::new(table),
            table,
            pred,
        }
    }
}
