use std::path::Path;

use crate::{
    catalog::object::ObjectSchema,
    error::DbResult,
    exec::query::{Executor, QueryCtx},
    io::{bootstrap, disk_manager::DiskManager, pager::Pager},
};

/// A `fdb` database instance.
pub struct Db {
    pager: Pager,
    schema: ObjectSchema,
}

impl Db {
    /// Opens a database "connection" and returns the instance. This method also
    /// bootstraps the database on the first access.
    ///
    /// On first access, `true` is returned as the second tuple element.
    pub async fn open(path: &Path) -> DbResult<(Self, bool)> {
        let disk_manager = DiskManager::new(Path::new(path)).await?;
        let mut pager = Pager::new(disk_manager);

        let (first_page_guard, is_new) = bootstrap::boot_first_page(&mut pager).await?;

        let first_page = first_page_guard.read().await;
        let schema = first_page.object_schema.clone();
        first_page.release();

        Ok((Db { pager, schema }, is_new))
    }

    /// Executes the given query, passing the callback closure for each yielded
    /// element.
    pub async fn execute<Q, E, F>(&self, mut query: Q, mut f: F) -> DbResult<Result<(), E>>
    where
        Q: Executor,
        F: for<'a> FnMut(Q::Item<'a>) -> Result<(), E>,
    {
        let ctx = QueryCtx {
            pager: &self.pager,
            object_schema: &self.schema,
        };
        while let Some(item) = query.next(&ctx).await? {
            if let error @ Err(_) = f(item) {
                return Ok(error);
            }
        }
        Ok(Ok(()))
    }

    /// Returns a reference to the database pager.
    ///
    /// This method is not stable and in the future will be removed in favor of
    /// a SQL interface.
    pub fn pager(&self) -> &Pager {
        &self.pager
    }
}