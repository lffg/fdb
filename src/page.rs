use std::num::NonZeroU32;

use buff::Buff;

use crate::{catalog, config::PAGE_SIZE, error::DbResult, ioutil::PageIdBuffExt};

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

/// The first page, which contains the database header and the "heap page" that
/// contains the database schema tuples.
///
/// The first 100 bytes are reserved for the header (although currently most of
/// it remains unused). The next [`PAGE_SIZE`] - 100 bytes are used to simulate
/// an usual [`HeapPage`].
///
/// The first 10 bytes are reserved for the ASCII string `"fdb format"`.
#[derive(Debug)]
pub struct FirstPage {
    /// The database header.
    header: DatabaseHeaderPageData,
    /// The database catalog that follows the 100-byte database header.
    catalog: CatalogPageData,
}

impl Page for FirstPage {
    fn id(&self) -> PageId {
        PageId::new(1.try_into().unwrap())
    }

    fn serialize(&self, buf: &mut Buff<'_>) {
        buf.scoped_exact(100, |buf| {
            let header = &self.header;

            buf.write_slice(b"fdb format");
            buf.write(header.file_format_version);
            buf.write(header.page_count);
            buf.write_page(header.first_free_list_page_id);

            let rest = 100 - buf.offset();
            buf.write_bytes(rest, 0);
        });

        // TODO: Write catalog.
    }

    fn deserialize(buf: &mut Buff<'_>) -> DbResult<Self> {
        let header = buf.scoped_exact(100, |buf| {
            buf.seek(10); // skip first 10 bytes, "fdb format"
            let header = DatabaseHeaderPageData {
                file_format_version: buf.read(),
                page_count: buf.read(),
                first_free_list_page_id: buf.read_page(),
            };
            debug_assert_eq!(buf.read::<1, u8>(), 0);
            buf.seek(100);
            header
        });
        Ok(FirstPage {
            header,
            ..Default::default()
        })
    }
}

impl Default for FirstPage {
    fn default() -> Self {
        Self {
            header: DatabaseHeaderPageData {
                file_format_version: 0,
                page_count: 0,
                first_free_list_page_id: None,
            },
            catalog: CatalogPageData {
                next_id: None,
                object_count: 0,
                objects: vec![],
            },
        }
    }
}

/// A heap page.
#[derive(Debug)]
pub struct HeapPage {
    id: PageId,
    next_id: PageId,
    ty: HeapPageType,
}

/// A [`HeapPage`] type.
#[derive(Debug)]
pub enum HeapPageType {
    WithSchema(SchemaPageData),
    Normal,
}

/// TODO: Implement this type of page.
#[allow(unused)]
#[derive(Debug)]
pub struct FreeListPage {
    id: PageId,
}

/// The database header.
#[derive(Debug)]
pub struct DatabaseHeaderPageData {
    /// The file format version. Currently, such a version is defined as `0`.
    file_format_version: u8,
    /// The total number of pages being used in the file.
    page_count: u32,
    /// The ID of the first free list page.
    first_free_list_page_id: Option<PageId>,
}

/// A catalog page wraps definitions of database objects.
///
/// The first catalog page is stored within the [`FirstPage`]. If the database
/// catalog can't fit in there, other catalog pages may be stored in heap pages;
/// hence, the `next_id` field.
#[derive(Debug)]
pub struct CatalogPageData {
    // TODO(P0): See if this representation fits in the slotted page approach.
    // It MUST since HEAP PAGES will work using an slotted page approach.
    next_id: Option<PageId>,
    object_count: u16,
    objects: Vec<catalog::Object>,
}

#[derive(Debug)]
pub struct SchemaPageData {
    column_count: u16,
    columns: Vec<catalog::Column>,
}
