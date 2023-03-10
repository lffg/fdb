use buff::Buff;

use crate::{
    catalog::page::{Page, PageId, PageType, SpecificPage},
    error::{DbResult, Error},
    util::io::{read_verify_eq, Serde, Size},
};

/// The database header size.
pub const HEADER_SIZE: usize = 100;

/// The first page, which contains the database header. Currently, the database
/// wastes `PAGE_SIZE - 100` bytes in space of the first page, for
/// simplification's sake. In the future, this region will be used to store the
/// first section of the database schema heap pages sequence.
///
/// The first 10 bytes are reserved for the ASCII string `"fdb format"`.
#[derive(Debug)]
pub struct FirstPage {
    /// The database header.
    pub header: MainHeader,
}

impl Size for FirstPage {
    fn size(&self) -> u32 {
        // One doesn't need to contabilize the type byte here, since the
        // database utilizes the `'f' as u8` code point as the first page's type
        // tag.
        self.header.size()
    }
}

impl Serde<'_> for FirstPage {
    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()> {
        self.header.serialize(buf)?;
        buf.pad_end_bytes(0);
        Ok(())
    }

    fn deserialize(buf: &mut Buff<'_>) -> DbResult<Self> {
        Ok(FirstPage {
            header: MainHeader::deserialize(buf)?,
        })
    }
}

impl SpecificPage for FirstPage {
    fn ty() -> PageType {
        PageType::First
    }

    fn id(&self) -> PageId {
        PageId::new_u32(1)
    }

    fn default_with_id(page_id: PageId) -> Self {
        assert_eq!(page_id.get(), 1, "first page must have page id 1");

        Self {
            header: MainHeader {
                file_format_version: 1,
                page_count: 1,
                first_free_list_page_id: None,
                first_schema_seq_page_id: PageId::new_u32(2),
            },
        }
    }

    super::impl_cast_methods!(Page::First => FirstPage);
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
    /// The ID of the first schema page.
    pub first_schema_seq_page_id: PageId,
}

impl Size for MainHeader {
    fn size(&self) -> u32 {
        HEADER_SIZE as u32
    }
}

impl Serde<'_> for MainHeader {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.scoped_exact(HEADER_SIZE, |buf| {
            buf.write_slice(b"fdb format");
            buf.write(self.file_format_version);
            buf.write(self.page_count);
            self.first_free_list_page_id.serialize(buf)?;
            self.first_schema_seq_page_id.serialize(buf)?;

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
                first_schema_seq_page_id: PageId::deserialize(buf)?,
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
