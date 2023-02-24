use std::io;

use bytes::BytesMut;

use crate::{
    config::PAGE_SIZE,
    disk_manager::DiskManager,
    page::{Page, PageId},
};

/// The pager, also known as buffer pool manager, is a central part of the
/// database storage engine, being responsible for deciding when and which pages
/// are to be loaded and flushed to the disk.
///
/// It also handles page caching and such a cache's eviction policies, which is
/// important to improve the database efficiency. However, this functionality is
/// not yet implemented.
pub struct Pager {
    /// The [`DiskManager`] instance.
    dm: DiskManager,
}

impl Pager {
    /// Creates a new pager.
    pub fn new(disk_manager: DiskManager) -> Self {
        Self { dm: disk_manager }
    }

    /// Given a page ID, fetches it from the disk.
    pub fn load<P: Page>(&mut self, id: PageId) -> io::Result<P> {
        // TODO: Use a buffer pool.
        let mut buf = BytesMut::zeroed(PAGE_SIZE as usize);

        self.dm.read_page(id, &mut buf)?;
        let page = P::deserialize(&mut buf)?;

        Ok(page)
    }
}
