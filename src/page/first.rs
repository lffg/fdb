use buff::Buff;

use crate::{
    error::{DbResult, Error},
    ioutil::{BuffExt, Serde},
    page::{catalog_data::CatalogData, main_header_data::MainHeaderData, Page, PageId},
};

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
    header: MainHeaderData,
    /// The database catalog that follows the 100-byte database header.
    catalog: CatalogData,
}

impl Page for FirstPage {
    fn id(&self) -> PageId {
        PageId::new(1.try_into().unwrap())
    }
}

impl Serde for FirstPage {
    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()> {
        buf.scoped_exact(100, |buf| {
            let header = &self.header;

            buf.write_slice(b"fdb format");
            buf.write(header.file_format_version);
            buf.write(header.page_count);
            buf.write_page_id(header.first_free_list_page_id);

            let rest = 98 - buf.offset();
            buf.write_bytes(rest, 0);
            buf.write_slice(br"\0");
        });

        // TODO: Write catalog.

        Ok(())
    }

    fn deserialize(buf: &mut Buff<'_>) -> DbResult<Self> {
        let header = buf.scoped_exact(100, |buf| {
            buf.read_verify_eq::<10>(*b"fdb format")
                .map_err(|_| Error::CorruptedHeader("start"))?; // header sig
            let header = MainHeaderData {
                file_format_version: buf.read(),
                page_count: buf.read(),
                first_free_list_page_id: buf.read_page_id(),
            };
            buf.seek(98);
            buf.read_verify_eq::<2>(*br"\0")
                .map_err(|_| Error::CorruptedHeader("end"))?; // finish header sig

            Ok::<_, Error>(header)
        })?;
        Ok(FirstPage {
            header,
            // TODO: Read catalog.
            ..Default::default()
        })
    }
}

impl Default for FirstPage {
    fn default() -> Self {
        Self {
            header: MainHeaderData {
                file_format_version: 0,
                page_count: 0,
                first_free_list_page_id: None,
            },
            catalog: CatalogData {
                next_id: None,
                object_count: 0,
                objects: vec![],
            },
        }
    }
}
