use async_trait::async_trait;
use tracing::instrument;

use crate::{
    catalog::object::TableObject,
    error::DbResult,
    exec::{
        query::{table::LinearScan, Query},
        values::Values,
    },
    Db,
};

/// A select query.
pub struct Select<'a> {
    linear_scan: LinearScan<'a>,
}

#[async_trait]
impl Query for Select<'_> {
    // TODO: Create ordered row abstraction (so that select return data in the
    // same order as the user requested).
    type Item<'a> = Values;

    #[instrument(name = "TableSelect", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        loop {
            let result = if let Some(record) = self.linear_scan.next(db).await? {
                if record.is_deleted() {
                    continue;
                }
                Some(record.into_data().into_owned().into_values())
            } else {
                None
            };
            return Ok(result);
        }
    }
}

impl<'a> Select<'a> {
    pub fn new(table: &'a TableObject) -> Select<'a> {
        Self {
            linear_scan: LinearScan::new(table),
        }
    }
}
