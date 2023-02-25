use std::num::NonZeroU32;

use buff::Buff;

use crate::{
    catalog,
    config::PAGE_SIZE,
    error::{DbResult, Error},
    ioutil::BuffExt,
};

// TODO: `free_list`.
pub mod first;

pub mod catalog_data;
pub mod main_header_data;
pub mod schema_data;

/// A contract that represents an in-memory page.
///
/// Since the database engine can interpret the "raw page" sequence of bytes,
/// there may be many types that map to a page in-memory. Though representing
/// different use-cases, all pages share some common functionality. For example,
/// all of them must expose an [`PageId`], be serializable and deserializable,
/// etc.
pub trait Page {
    /// Returns the corresponding [`PageId`].
    fn id(&self) -> PageId;

    // TODO: Maybe extract these next two to a "serialize/deserialize" trait.
    /// Serializes the page.
    ///
    /// This operation shouldn't fail.
    fn serialize(&self, buf: &mut Buff<'_>);

    /// Deserializes the page.
    // TODO: Maybe use an associated type to encode the error.
    fn deserialize(buf: &mut Buff<'_>) -> DbResult<Self>
    where
        Self: Sized;
}

/// A wrapper that represents a page id.
///
/// Although disk offsets start from zero, this implementation considers the
/// first page at index 1. This allows using the 0-value to encode NULL pages,
/// i.e., a reference to a page that doesn't exist. Indeed, this same approach
/// is used by DBMSs such as SQLite.
#[derive(Copy, Clone, Debug)]
#[repr(transparent)]
pub struct PageId(NonZeroU32);

impl PageId {
    /// Constructs a new [`PageId`] using the given page number.
    pub fn new(page_number: NonZeroU32) -> Self {
        PageId(page_number)
    }

    /// Returns the underlying page id.
    pub fn get(self) -> u32 {
        self.0.get()
    }

    /// Returns the 0-based page offset, commonly used in disk seek operations.
    #[inline]
    pub fn offset(self) -> u64 {
        (self.0.get() as u64 - 1) * PAGE_SIZE
    }
}
