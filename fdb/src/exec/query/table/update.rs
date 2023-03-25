use std::borrow::Cow;

use async_trait::async_trait;
use tracing::{debug, instrument};

use crate::{
    catalog::{object::TableObject, page::HeapPage, record::simple_record},
    error::DbResult,
    exec::{
        query::{self, table::LinearScan, Query},
        values::Values,
    },
    util::io::SerializeCtx,
    Db,
};

/// The update predicate.
pub type Pred = dyn Sync + for<'v> Fn(&'v Values) -> bool;

/// The updater function.
pub type Updater = dyn Sync + for<'v> Fn(&'v mut Values);

/// An update query.
pub struct Update<'a> {
    table: &'a TableObject,
    linear_scan: LinearScan<'a>,
    pred: &'a Pred,
    updater: &'a Updater,
}

#[async_trait]
impl Query for Update<'_> {
    // TODO: Add `updated_count`.
    type Item<'a> = ();

    #[instrument(name = "TableUpdate", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        loop {
            let out = if let Some(mut record) = self.linear_scan.next(db).await? {
                let schema = &self.table.schema;
                let values = record.as_data().as_values();

                if record.is_deleted() || !(self.pred)(values) {
                    continue;
                }

                let page_id = record.page_id();
                let offset = record.offset();
                debug!(?page_id, "allocating page for write");
                let guard = db.pager().get::<HeapPage>(page_id).await?;
                let mut page = guard.write().await;

                // Clone the current row and modify it.
                let mut values = record.as_data().as_values().clone();
                (self.updater)(&mut values);
                let schematized_values = Cow::Owned(values.try_into_schematized(schema)?);

                let serde_ctx = simple_record::TableRecordCtx {
                    page_id,
                    offset,
                    schema,
                };

                match record.try_update(schematized_values) {
                    Ok(_) => {
                        debug!("updated in place");
                        page.write_at(offset, |buf| record.serialize(buf, &serde_ctx))?;
                        page.flush();
                    }
                    Err(new_data) => {
                        debug!("new record didn't fit; allocating new space");

                        record.set_deleted();
                        page.write_at(offset, |buf| record.serialize(buf, &serde_ctx))?;
                        // Must flush before executing `Insert`. Otherwise, deadlock. t-t
                        page.flush();

                        let values = new_data.into_owned().into_values();
                        let mut ins = query::table::Insert::new(self.table, values);
                        ins.next(db).await?;
                    }
                }

                Some(())
            } else {
                db.pager().flush_all().await?;
                None
            };
            return Ok(out);
        }
    }
}

impl<'s> Update<'s> {
    pub fn new(table: &'s TableObject, pred: &'s Pred, updater: &'s Updater) -> Update<'s> {
        Self {
            table,
            linear_scan: LinearScan::new(table),
            pred,
            updater,
        }
    }
}
