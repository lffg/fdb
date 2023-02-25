use buff::Buff;

use crate::page::PageId;

pub trait PageIdBuffExt {
    /// Reads an optional page id. If the underlying data is 0, `None` is
    /// returned.
    fn read_page(&mut self) -> Option<PageId>;

    /// Writes an optional page id. If `None` is provided, 0 is written.
    fn write_page(&mut self, page: Option<PageId>);
}

impl PageIdBuffExt for Buff<'_> {
    /// Reads an optional page id. If the underlying data is 0, `None` is
    /// returned.
    fn read_page(&mut self) -> Option<PageId> {
        match self.read() {
            0_u32 => None,
            num => Some(PageId::new(num.try_into().expect("non-zero page id"))),
        }
    }

    /// Writes an optional page id. If `None` is provided, 0 is written.
    fn write_page(&mut self, page: Option<PageId>) {
        let num = page.map(PageId::get).unwrap_or(0);
        self.write(num)
    }
}
