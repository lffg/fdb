use std::num::NonZeroU32;

use crate::{config::PAGE_SIZE, ioutil::Serde};

// TODO: `free_list`.
pub mod first;
pub mod heap;

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
///
/// All `Page` implementation must also implement [`Serde`], so that they may be
/// serialized and deserialized.
pub trait Page: Serde {
    /// Returns the corresponding [`PageId`].
    fn id(&self) -> PageId;
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

    /// Constructs a new [`PageId`] using the given page number.
    ///
    /// Panics if received zero.
    pub fn new_u32(page_number: u32) -> Self {
        Self::new(page_number.try_into().unwrap())
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

/// Represents a state in which a page may be returned (e.g. by the
/// [`crate::Pager`]).
pub enum PageState<P> {
    New(P),
    Existing(P),
}

impl<P> PageState<P> {
    // Uncomment when needed
    // =====================
    // /// Returns the underlying page.
    // pub fn into_inner(self) -> P {
    //     match self {
    //         PageState::New(inner) | PageState::Existing(inner) => inner,
    //     }
    // }

    /// Returns a reference to the underlying page.
    pub fn get(&self) -> &P {
        match &self {
            PageState::New(inner) | PageState::Existing(inner) => inner,
        }
    }
}
