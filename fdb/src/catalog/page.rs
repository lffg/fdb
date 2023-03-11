use std::{
    num::NonZeroU32,
    ops::{Add, AddAssign},
};

use tracing::error;

use crate::{
    error::{DbResult, Error},
    util::io::{Serde, Size},
};

/// The first page definition.
mod first;
pub use first::*;

/// The heap page definition.
mod heap;
pub use heap::*;

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
            Page::First(_) => FirstPage::ty(),
            Page::Heap(_) => HeapPage::ty(),
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

impl Size for Page {
    fn size(&self) -> u32 {
        match self {
            Page::First(inner) => inner.size(),
            Page::Heap(inner) => inner.size(),
        }
    }
}

impl Serde<'_> for Page {
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

impl Size for PageType {
    fn size(&self) -> u32 {
        1
    }
}

impl Serde<'_> for PageType {
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
    /// The first page ID.
    pub const FIRST: PageId = PageId::new_u32(1);

    /// Constructs a new [`PageId`] using the given page number.
    pub const fn new(page_number: NonZeroU32) -> Self {
        PageId(page_number)
    }

    /// Constructs a new [`PageId`] using the given page number.
    ///
    /// Panics if received zero.
    pub const fn new_u32(page_number: u32) -> Self {
        if page_number == 0 {
            panic!("cannot construct page number with 0");
        }
        PageId(unsafe { NonZeroU32::new_unchecked(page_number) })
    }

    /// Returns the underlying page id.
    pub const fn get(self) -> u32 {
        self.0.get()
    }

    /// Returns the 0-based page offset, commonly used in disk seek operations.
    #[inline]
    pub const fn offset(self, page_size: u16) -> u64 {
        (self.0.get() as u64 - 1) * page_size as u64
    }
}

impl Add<u32> for PageId {
    type Output = PageId;

    fn add(self, rhs: u32) -> Self::Output {
        PageId::new_u32(self.get() + rhs)
    }
}

impl AddAssign<u32> for PageId {
    fn add_assign(&mut self, rhs: u32) {
        *self = *self + rhs
    }
}

impl Size for PageId {
    fn size(&self) -> u32 {
        4
    }
}

impl Serde<'_> for PageId {
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

impl Size for Option<PageId> {
    fn size(&self) -> u32 {
        4
    }
}

impl Serde<'_> for Option<PageId> {
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

/// Specific page types.
pub trait SpecificPage: Sized + for<'a> Serde<'a> {
    /// Returns the [`PageType`].
    fn ty() -> PageType;

    /// Returns the [`PageId`].
    fn id(&self) -> PageId;

    /// Converts the specific page type into [`Page`].
    fn into_page(self) -> Page;

    /// Casts a [`Page`] to the specific type.
    fn cast(page: Page) -> Self;

    /// Casts a [`Page`] reference to the specific type.
    fn cast_ref(page: &Page) -> &Self;

    /// Casts a [`Page`] mutable reference to the specific type.
    fn cast_mut(page: &mut Page) -> &mut Self;
}

impl SpecificPage for Page {
    fn ty() -> PageType {
        unimplemented!("must call on specific page type");
    }

    fn id(&self) -> PageId {
        self.id()
    }

    #[inline(always)]
    fn into_page(self) -> Self {
        self
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

/// Convention macro to ease the implementation of [`SpecificPage`]'s cast
/// methods.
macro_rules! impl_cast_methods {
    ($page:ident :: $variant:ident => $target:ty) => {
        fn into_page(self) -> Page {
            $page::$variant(self)
        }

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
