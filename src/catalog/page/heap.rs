//! Heap pages store records in an unordered and sequential fashion.

use crate::{
    catalog::page::{Page, PageId},
    config::PAGE_SIZE,
    error::{DbResult, Error},
    ioutil::{BuffExt, Serde},
};

/// The size of the header of the first heap page.
pub const FIRST_HEAP_PAGE_HEADER_SIZE: u16 = ORDINARY_HEAP_PAGE_HEADER_SIZE + 16;

/// The size of the header of ordinary heap pages.
pub const ORDINARY_HEAP_PAGE_HEADER_SIZE: u16 = 12;

/// The first [`HeapPage`] in the sequence.
#[derive(Debug)]
pub struct FirstHeapPage {
    // NOTE: The order of fields in this struct definition IS NOT the same of
    // the order in the disk format. See the Serde implementation for the
    // normative ordering.
    /// The ID of the last page in this sequence.
    pub last_page_id: PageId,
    /// The number of pages in this sequence.
    pub total_page_count: u32,
    /// The number of records in this sequence.
    pub total_record_count: u64,
    // Since `FirstHeapPage` is a superset of `OrdinaryHeapPage`s, one may store
    // it directly here.
    /// The ordinary heap fields.
    pub ordinary_page: OrdinaryHeapPage,
}

impl Page for FirstHeapPage {
    fn id(&self) -> PageId {
        self.ordinary_page.id
    }
}

impl Serde for FirstHeapPage {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.scoped_exact(FIRST_HEAP_PAGE_HEADER_SIZE as usize, |buf| {
            // common fields
            buf.write_page_id(Some(self.ordinary_page.id));
            buf.write_page_id(self.ordinary_page.next_page_id);
            buf.write(self.ordinary_page.record_count);
            buf.write(self.ordinary_page.free_offset);
            // first page only header fields
            buf.write_page_id(Some(self.last_page_id));
            buf.write(self.total_page_count);
            buf.write(self.total_record_count);
            Ok::<_, Error>(())
        })?;
        buf.write_slice(&self.ordinary_page.bytes);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        // common fields
        let id = buf.read_page_id().expect("current page id");
        let next_page_id = buf.read_page_id();
        let record_count: u16 = buf.read();
        let free_offset: u16 = buf.read();
        // first page only header fields
        let last_page_id = buf.read_page_id().expect("last page id");
        let total_page_count: u32 = buf.read();
        let total_record_count: u64 = buf.read();

        let mut bytes = vec![0; buf.remaining()]; // TODO: Optimize using `MaybeUninit`.
        buf.read_slice(&mut bytes);

        Ok(FirstHeapPage {
            last_page_id,
            total_page_count,
            total_record_count,
            ordinary_page: OrdinaryHeapPage {
                id,
                next_page_id,
                record_count,
                free_offset,
                bytes,
            },
        })
    }
}

impl FirstHeapPage {
    /// Constructs a new page.
    pub fn new(page_id: PageId) -> Self {
        let mut page = FirstHeapPage {
            last_page_id: page_id,
            total_page_count: 1,
            total_record_count: 0,
            ordinary_page: OrdinaryHeapPage::new(page_id),
        };
        page.ordinary_page.free_offset = FIRST_HEAP_PAGE_HEADER_SIZE;
        page
    }
}

/// Ordinary heap page.
#[derive(Debug)]
pub struct OrdinaryHeapPage {
    pub id: PageId,
    pub next_page_id: Option<PageId>,
    /// Element count in this page.
    pub record_count: u16,
    /// Offset of the free bytes section.
    pub free_offset: u16,
    pub bytes: Vec<u8>, // TODO: Review this.
}

impl Page for OrdinaryHeapPage {
    fn id(&self) -> PageId {
        self.id
    }
}

impl Serde for OrdinaryHeapPage {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.scoped_exact(ORDINARY_HEAP_PAGE_HEADER_SIZE as usize, |buf| {
            buf.write_page_id(Some(self.id));
            buf.write_page_id(self.next_page_id);
            buf.write(self.record_count);
            buf.write(self.free_offset);
            Ok::<_, Error>(())
        })?;
        buf.write_slice(&self.bytes);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let id = buf.read_page_id().expect("current page id");
        let next_page_id = buf.read_page_id();
        let record_count: u16 = buf.read();
        let free_offset: u16 = buf.read();

        let mut bytes = vec![0; buf.remaining()]; // TODO: Optimize using `MaybeUninit`.
        buf.read_slice(&mut bytes);

        Ok(OrdinaryHeapPage {
            id,
            next_page_id,
            record_count,
            free_offset,
            bytes,
        })
    }
}

impl OrdinaryHeapPage {
    /// Constructs a new page.
    pub fn new(page_id: PageId) -> Self {
        OrdinaryHeapPage {
            id: page_id,
            next_page_id: None,
            record_count: 0,
            free_offset: ORDINARY_HEAP_PAGE_HEADER_SIZE,
            bytes: vec![],
        }
    }

    /// Returns the remaining number of bytes in the record section.
    pub fn remaining(&self) -> u64 {
        PAGE_SIZE - self.free_offset as u64
    }
}
