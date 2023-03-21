use std::path::Path;

use crate::{
    error::DbResult,
    exec::query::Query,
    io::{bootstrap, disk_manager::DiskManager, pager::Pager},
};

/// The default database page size.
pub const DEFAULT_PAGE_SIZE: u16 = 4 * 1024;

/// A `fdb` database instance.
pub struct Db {
    pager: Pager,
}

impl Db {
    /// Opens a database "connection" and returns the instance. This method also
    /// bootstraps the database on the first access.
    ///
    /// On first access, `true` is returned as the second tuple element.
    pub async fn open(path: &Path) -> DbResult<(Self, bool)> {
        Self::open_with_page_size(path, DEFAULT_PAGE_SIZE).await
    }

    /// Same as [`Db::open`], but allows for setting a different page size.
    pub async fn open_with_page_size(path: &Path, page_size: u16) -> DbResult<(Self, bool)> {
        let disk_manager = DiskManager::new(Path::new(path), page_size).await?;
        let mut pager = Pager::new(disk_manager);

        let is_new = bootstrap::boot_first_page(&mut pager).await?;
        Ok((Db { pager }, is_new))
    }

    /// Executes the given query, passing the callback closure for each yielded
    /// element.
    pub async fn execute<Q, F, E>(&self, mut query: Q, mut f: F) -> DbResult<Result<(), E>>
    where
        Q: Query,
        F: for<'a> FnMut(Q::Item<'a>) -> Result<(), E>,
    {
        while let Some(item) = query.next(self).await? {
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

    /// Returns the database's page size.
    pub fn page_size(&self) -> u16 {
        self.pager.page_size()
    }
}
