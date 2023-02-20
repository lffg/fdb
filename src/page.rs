use crate::config::PAGE_SIZE;

/// A wrapper that represents a page id.
///
/// Although disk offsets start from zero, this implementation considers the
/// first page at index 1. This allows using the 0-value to encode NULL pages,
/// i.e., a reference to a page that doesn't exist. Indeed, this same approach
/// is used by DBMSs such as SQLite.
#[derive(Copy, Clone)]
pub struct PageId(u32);

impl PageId {
    /// Constructs a new [`PageId`] using the given page number.
    pub fn new(page_number: u32) -> Self {
        PageId(page_number)
    }

    /// Alias for `PageId::new(0)`.
    pub fn null() -> Self {
        PageId::new(0)
    }

    /// Returns the 0-based page offset, commonly used in disk seek operations.
    #[inline]
    pub fn offset(self) -> u64 {
        (self.0 as u64 - 1) * PAGE_SIZE
    }
}
