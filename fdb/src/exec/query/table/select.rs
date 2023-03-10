use async_trait::async_trait;

use crate::{
    error::DbResult,
    exec::{
        query::{table::LinearScan, Query, QueryCtx},
        values::Values,
    },
};

/// An select query.
pub struct Select<'a> {
    linear_scan: LinearScan<'a>,
}

#[async_trait]
impl Query for Select<'_> {
    // TODO: Create ordered row abstraction (so that select return data in the
    // same order as the user requested).
    type Item<'a> = Values;

    async fn next<'a>(&mut self, ctx: &'a QueryCtx<'a>) -> DbResult<Option<Self::Item<'a>>> {
        loop {
            let result = if let Some(record) = self.linear_scan.next(ctx).await? {
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

impl<'s> Select<'s> {
    /// Creates a new insert executor.
    pub fn new(table_name: &'s str) -> Select<'s> {
        Self {
            linear_scan: LinearScan::new(table_name),
        }
    }
}
