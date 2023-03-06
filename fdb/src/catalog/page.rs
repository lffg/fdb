use std::num::NonZeroU32;

use crate::{
    config::PAGE_SIZE,
    error::{DbResult, Error},
    util::io::Serde,
};

/// The first page definition.
mod first;
pub use first::*;

/// The heap page definition.
mod heap;
pub use heap::*;
use tracing::error;

/// An in-memory page.
///
/// Since the database engine can interpret the "raw page" sequence of bytes,
/// there may be many types that map to a page in-memory. Though representing
/// different use-cases, all pages share some common functionality. For example,
/// all of them must expose an [`PageId`], be serializable and deserializable,
/// etc.
///
/// All "usable" page implementations of the database may be wrapped in this
/// enum.
#[derive(Debug)]
pub enum Page {
    First(FirstPage),
    Heap(HeapPage),
}

impl Page {
    /// Returns the [`PageId`].
    pub fn id(&self) -> PageId {
        match self {
            Page::First(inner) => inner.id(),
            Page::Heap(inner) => inner.id(),
        }
    }

    /// Returns the [`PageType`]. It is always encoded in the FIRST byte of the
    /// page.
    pub fn ty(&self) -> PageType {
        match self {
            Page::First(inner) => inner.ty(),
            Page::Heap(inner) => inner.ty(),
        }
    }

    /// Casts the page reference to a specific page type.
    #[inline]
    pub fn cast<T: SpecificPage>(self) -> T {
        T::cast(self)
    }

    /// Casts the page reference to a specific page reference type.
    #[inline]
    pub fn cast_ref<T: SpecificPage>(&self) -> &T {
        T::cast_ref(self)
    }

    /// Casts the page mutable reference to a specific page mutable reference
    /// type.
    #[inline]
    pub fn cast_mut<T: SpecificPage>(&mut self) -> &mut T {
        T::cast_mut(self)
    }
}

impl Serde<'_> for Page {
    fn size(&self) -> u32 {
        PAGE_SIZE as u32
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        match self {
            Page::First(inner) => inner.serialize(buf),
            Page::Heap(inner) => inner.serialize(buf),
        }
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        debug_assert_eq!(buf.offset(), 0);

        let ty = PageType::deserialize(buf)?;
        buf.seek(0);

        Ok(match ty {
            PageType::First => Page::First(FirstPage::deserialize(buf)?),
            PageType::Heap => Page::Heap(HeapPage::deserialize(buf)?),
        })
    }
}

/// The page type.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// See [`FirstPage`].
    ///
    /// `0x66` represents the lowercase `f` character, the first byte of the
    /// first page's global header `"fdb format"` sequence.
    First = 0x66,
    /// See [`HeapPage`].
    Heap = 0x01,
}

impl Serde<'_> for PageType {
    fn size(&self) -> u32 {
        1
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(*self as u8);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let tag: u8 = buf.read();
        match tag {
            0x66 => Ok(PageType::First),
            0x01 => Ok(PageType::Heap),
            unexpected => {
                error!(?unexpected, "invalid `PageType` type discriminant");
                Err(Error::CorruptedTypeTag)
            }
        }
    }
}

impl PageType {
    /// Returns the tag associated with the `HeapPageId`.
    pub const fn discriminant(self) -> u8 {
        self as u8
    }
}

/// A wrapper that represents a page id.
///
/// Although disk offsets start from zero, this implementation considers the
/// first page at index 1. This allows using the 0-value to encode NULL pages,
/// i.e., a reference to a page that doesn't exist. Indeed, this same approach
/// is used by DBMSs such as SQLite.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
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

impl Serde<'_> for PageId {
    fn size(&self) -> u32 {
        4
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        Some(*self).serialize(buf)
    }

    /// Must not try to deserialize a null page ID.
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(Option::<Self>::deserialize(buf)?.expect("non null page id"))
    }
}

impl Serde<'_> for Option<PageId> {
    fn size(&self) -> u32 {
        4
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        let num = self.map(PageId::get).unwrap_or(0);
        buf.write(num);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let num = buf.read();
        Ok(NonZeroU32::new(num).map(PageId::new))
    }
}

/// Represents a state in which a page may be returned (e.g. by the
/// [`crate::Pager`]).
pub enum PageState<P> {
    New(P),
    Existing(P),
}

impl<P> PageState<P> {
    /// Returns the underlying page.
    pub fn _into_inner(self) -> P {
        match self {
            PageState::New(inner) | PageState::Existing(inner) => inner,
        }
    }

    /// Returns a reference to the underlying page.
    pub fn get(&self) -> &P {
        match &self {
            PageState::New(inner) | PageState::Existing(inner) => inner,
        }
    }
}

/// Specific page types.
pub trait SpecificPage {
    /// Returns the [`PageId`].
    fn id(&self) -> PageId;

    /// Returns the [`PageType`].
    fn ty(&self) -> PageType;

    /// Casts a [`Page`] to the specific type.
    ///
    /// The conversion is infallible. An error in a cast indicates a logic bug
    /// and, as such, panics.
    fn cast(page: Page) -> Self;
    fn cast_ref(page: &Page) -> &Self;
    fn cast_mut(page: &mut Page) -> &mut Self;
}

impl SpecificPage for Page {
    fn id(&self) -> PageId {
        self.id()
    }

    fn ty(&self) -> PageType {
        self.ty()
    }

    #[inline(always)]
    fn cast(page: Page) -> Self {
        page
    }

    #[inline(always)]
    fn cast_ref(page: &Page) -> &Self {
        page
    }

    #[inline(always)]
    fn cast_mut(page: &mut Page) -> &mut Self {
        page
    }
}

macro_rules! impl_cast_methods {
    ($page:ident :: $variant:ident => $target:ty) => {
        fn cast(page: Page) -> $target {
            if let $page::$variant(inner) = page {
                inner
            } else {
                unreachable!();
            }
        }
        fn cast_ref(page: &Page) -> &$target {
            if let $page::$variant(inner) = page {
                inner
            } else {
                unreachable!();
            }
        }
        fn cast_mut(page: &mut Page) -> &mut $target {
            if let $page::$variant(inner) = page {
                inner
            } else {
                unreachable!();
            }
        }
    };
}
use impl_cast_methods;
