use std::num::NonZeroU32;

use buff::Buff;

use crate::{
    error::{DbResult, Error},
    page::PageId,
};

pub trait BuffExt {
    /// Reads an optional page id. If the underlying data is 0, `None` is
    /// returned.
    fn read_page(&mut self) -> Option<PageId>;

    /// Writes an optional page id. If `None` is provided, 0 is written.
    fn write_page(&mut self, page: Option<PageId>);

    /// Reads `N` bytes and compares it to the given slice.
    fn read_verify_eq<const N: usize>(&mut self, expected: [u8; N]) -> Result<(), ()>;
}

impl BuffExt for Buff<'_> {
    fn read_page(&mut self) -> Option<PageId> {
        let num = self.read();
        NonZeroU32::new(num).map(PageId::new)
    }

    fn write_page(&mut self, page: Option<PageId>) {
        let num = page.map(PageId::get).unwrap_or(0);
        self.write(num);
    }

    fn read_verify_eq<const N: usize>(&mut self, expected: [u8; N]) -> Result<(), ()> {
        let mut actual = [0; N];
        self.read_slice(&mut actual);

        if actual == expected {
            Ok(())
        } else {
            Err(())
        }
    }
}
