use buff::Buff;

use crate::{
    config::PAGE_SIZE,
    disk_manager::DiskManager,
    error::DbResult,
    page::{Page, PageId},
};

/// The pager, also known as buffer pool manager or as page cache, is a central
/// part of the database storage engine, being responsible for deciding when and
/// which pages are to be loaded and flushed to the disk.
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
    pub fn load<P: Page>(&mut self, id: PageId) -> DbResult<P> {
        // TODO: Use a buffer pool.
        let mut buf = Box::new([0; PAGE_SIZE as usize]);
        let mut buf = Buff::new(&mut *buf);

        self.dm.read_page(id, buf.get_mut())?;
        let page = P::deserialize(&mut buf)?;

        Ok(page)
    }

    /// Immediately writes the given page on the disk.
    pub fn write_flush(&mut self, page: &dyn Page) -> DbResult<()> {
        // TODO: Use a buffer pool.
        let mut buf = Box::new([0; PAGE_SIZE as usize]);
        let mut buf = Buff::new(&mut *buf);

        page.serialize(&mut buf)?;
        self.dm.write_page(page.id(), buf.get())
    }
}
