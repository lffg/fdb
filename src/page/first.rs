use buff::Buff;

use crate::{
    error::DbResult,
    ioutil::Serde,
    page::{catalog_data::CatalogData, main_header_data::MainHeaderData, Page, PageId},
};

/// The first page, which contains the database header and the "heap page" that
/// contains the database schema tuples.
///
/// The first 100 bytes are reserved for the header (although currently most of
/// it remains unused). The next `PAGE_SIZE - 100` bytes are used to simulate an
/// usual heap page.
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
        self.header.serialize(buf)?;
        self.catalog.serialize(buf)?;
        Ok(())
    }

    fn deserialize(buf: &mut Buff<'_>) -> DbResult<Self> {
        Ok(FirstPage {
            header: MainHeaderData::deserialize(buf)?,
            catalog: CatalogData::deserialize(buf)?,
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
