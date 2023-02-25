use std::num::NonZeroU32;

use buff::Buff;

use crate::page::PageId;

pub trait BuffExt {
    /// Reads an optional page id. If the underlying data is 0, `None` is
    /// returned.
    fn read_page(&mut self) -> Option<PageId>;

    /// Writes an optional page id. If `None` is provided, 0 is written.
    fn write_page(&mut self, page: Option<PageId>);
}

impl BuffExt for Buff<'_> {
    /// Reads an optional page id. If the underlying data is 0, `None` is
    /// returned.
    fn read_page(&mut self) -> Option<PageId> {
        let num = self.read();
        NonZeroU32::new(num).map(PageId::new)
    }

    /// Writes an optional page id. If `None` is provided, 0 is written.
    fn write_page(&mut self, page: Option<PageId>) {
        let num = page.map(PageId::get).unwrap_or(0);
        self.write(num);
    }
}
