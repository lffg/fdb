use buff::Buff;

use crate::{
    catalog::{
        object::ObjectSchema,
        page::{Page, PageId, PageType, SpecificPage},
    },
    error::{DbResult, Error},
    util::io::{read_verify_eq, Serde},
};

/// The database header size.
pub const HEADER_SIZE: usize = 100;

/// The first page, which contains the database header and the "heap page" that
/// contains the database schema tuples.
///
/// The first 100 bytes are reserved for the header (although currently most of
/// it remains unused). The next `PAGE_SIZE - HEADER_SIZE` bytes are used to
/// simulate an usual heap page.
///
/// The first 10 bytes are reserved for the ASCII string `"fdb format"`.
#[derive(Debug)]
pub struct FirstPage {
    /// The database header.
    pub header: MainHeader,
    /// The database object schema that follows the 100-byte main header.
    pub object_schema: ObjectSchema,
}

impl Serde<'_> for FirstPage {
    fn size(&self) -> u32 {
        // One doesn't need to contabilize the type byte here, since the
        // database utilizes the `'f' as u8` code point as the first page's type
        // tag.
        self.header.size() + self.object_schema.size()
    }

    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()> {
        self.header.serialize(buf)?;
        self.object_schema.serialize(buf)?;
        Ok(())
    }

    fn deserialize(buf: &mut Buff<'_>) -> DbResult<Self> {
        Ok(FirstPage {
            header: MainHeader::deserialize(buf)?,
            object_schema: ObjectSchema::deserialize(buf)?,
        })
    }
}

impl SpecificPage for FirstPage {
    fn id(&self) -> PageId {
        PageId::new_u32(1)
    }

    fn ty(&self) -> PageType {
        PageType::First
    }

    super::impl_cast_methods!(Page::First => FirstPage);
}

impl FirstPage {
    /// Constructs a new page.
    pub fn new() -> Self {
        Self {
            header: MainHeader {
                file_format_version: 1,
                page_count: 1,
                first_free_list_page_id: None,
            },
            object_schema: ObjectSchema {
                next_id: None,
                objects: vec![],
            },
        }
    }
}

impl Default for FirstPage {
    fn default() -> Self {
        Self::new()
    }
}

/// The database header.
#[derive(Debug)]
pub struct MainHeader {
    /// The file format version. Currently, such a version is defined as `0`.
    pub file_format_version: u8,
    /// The total number of pages being used in the file.
    pub page_count: u32,
    /// The ID of the first free list page.
    pub first_free_list_page_id: Option<PageId>,
}

impl Serde<'_> for MainHeader {
    fn size(&self) -> u32 {
        HEADER_SIZE as u32
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.scoped_exact(HEADER_SIZE, |buf| {
            buf.write_slice(b"fdb format");
            buf.write(self.file_format_version);
            buf.write(self.page_count);
            self.first_free_list_page_id.serialize(buf)?;

            let rest = HEADER_SIZE - 2 - buf.offset();
            buf.write_bytes(rest, 0);
            buf.write_slice(br"\0");

            Ok::<_, Error>(())
        })
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        buf.scoped_exact(HEADER_SIZE, |buf| {
            // header sig
            if !read_verify_eq(buf, b"fdb format") {
                return Err(Error::CorruptedHeader("start"));
            }

            let header = MainHeader {
                file_format_version: buf.read(),
                page_count: buf.read(),
                first_free_list_page_id: Option::<PageId>::deserialize(buf)?,
            };

            buf.seek(HEADER_SIZE - 2);
            // finish header sig
            if !read_verify_eq(buf, br"\0") {
                return Err(Error::CorruptedHeader("end"));
            }

            Ok(header)
        })
    }
}
