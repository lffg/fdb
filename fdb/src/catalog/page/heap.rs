//! Heap pages store records in an unordered and sequential fashion.

use tracing::{error, trace};

use crate::{
    catalog::page::{Page, PageId, PageType, SpecificPage},
    error::{DbResult, Error},
    util::io::{Deserialize, Serialize, Size},
};

/// The first [`HeapPage`] in the sequence.
#[derive(Debug)]
pub struct HeapPage {
    /// The page header.
    pub header: Header,
    /// The record bytes in the page.
    pub bytes: Vec<u8>, // XX: Review this.
}

impl Size for HeapPage {
    fn size(&self) -> u32 {
        self.header.size() + self.bytes.len() as u32
    }
}

impl Serialize for HeapPage {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.header.serialize(buf)?;
        buf.write_slice(&self.bytes);
        buf.pad_end_bytes(0);

        Ok(())
    }
}

impl Deserialize<'_> for HeapPage {
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        assert_eq!(PageType::deserialize(buf)?, PageType::Heap);
        Ok(HeapPage {
            header: Header::deserialize(buf)?,
            bytes: {
                let mut bytes = vec![0; buf.remaining()];
                buf.read_slice(&mut bytes);
                bytes
            },
        })
    }
}

impl SpecificPage for HeapPage {
    fn ty() -> PageType {
        PageType::Heap
    }

    fn id(&self) -> PageId {
        self.header.id
    }

    super::impl_cast_methods!(Page::Heap => HeapPage);
}

impl HeapPage {
    /// Checks whether the page can accommodate `n` more bytes.
    pub fn can_accommodate(&self, n: u32) -> bool {
        // TODO(buff-trait): Use Buff API here instead.
        self.bytes.len() >= self.header.free_offset as usize + n as usize
    }

    /// Writes using the given closure.
    ///
    /// Changes the underlying data and the underlying free_offset marker. NOTE
    /// THAT THIS METHOD DOESN'T ALTER THE UNDERLYING RECORD COUNTER.
    pub fn write<F, R>(&mut self, f: F) -> DbResult<R>
    where
        F: for<'a> FnOnce(&mut buff::Buff<'a>) -> DbResult<R>,
    {
        trace!(page_id = ?self.id(), "writing to buffer");
        let mut buf = buff::Buff::new(&mut self.bytes[self.header.free_offset as usize..]);
        let start = buf.offset();
        let r = f(&mut buf)?;
        let delta = buf.offset() - start;
        self.header.free_offset += delta as u16;
        Ok(r)
    }

    /// Writes at the given position.
    ///
    /// Changes the underlying data. NOTE THAT THIS METHOD DOESN'T ALTER THE
    /// UNDERLYING `free_offset` MARKER AND THE UNDERLYING RECORD COUNTER.
    pub fn write_at<F, R>(&mut self, offset: u16, f: F) -> DbResult<R>
    where
        F: for<'a> FnOnce(&mut buff::Buff<'a>) -> DbResult<R>,
    {
        trace!(page_id = ?self.id(), "writing to buffer");
        let mut buf = buff::Buff::new(&mut self.bytes[offset as usize..]);
        let r = f(&mut buf)?;
        Ok(r)
    }

    /// Reads at the given offset.
    pub fn read_at<F, R>(&self, offset: u16, f: F) -> DbResult<R>
    where
        F: for<'a> FnOnce(&mut buff::Buff<'a>) -> DbResult<R>,
    {
        trace!(page_id = ?self.id(), "reading from buffer");
        // TODO: HACK: One must be able to create a buf from a shared slice.
        // TODO(buff-trait): Fix.
        let mut cloned_buf = self.bytes[offset as usize..].to_owned();
        let mut buf = buff::Buff::new(&mut cloned_buf);
        f(&mut buf)
    }

    /// Returns the initial data offset for this page's type.
    pub fn first_offset(&self) -> u16 {
        0
    }

    /// Returns the current offset.
    pub fn offset(&self) -> u16 {
        self.header.free_offset
    }

    /// Constructs the first page of a heap page sequence.
    pub fn new_seq_first(page_size: u16, page_id: PageId) -> Self {
        let header = Header {
            id: page_id,
            seq_header: Some(SeqHeader {
                last_page_id: page_id,
                page_count: 1,
                record_count: 0,
            }),
            next_page_id: None,
            record_count: 0,
            free_offset: 0,
        };
        let bytes = vec![0; page_size as usize - header.size() as usize];

        Self { header, bytes }
    }

    /// Constructs a heap page sequence node (i.e., not the first).
    pub fn new_seq_node(page_size: u16, page_id: PageId) -> Self {
        let header = Header {
            id: page_id,
            seq_header: None,
            next_page_id: Some(page_id),
            record_count: 0,
            free_offset: 0,
        };
        let bytes = vec![0; page_size as usize - header.size() as usize];

        Self { header, bytes }
    }
}

/// The [`HeapPage`] header. Not to be confused with [`SeqHeader`].
#[derive(Debug)]
pub struct Header {
    // Do not forget:
    // page_type: TypeId,
    /// The ID of the page.
    pub id: PageId,
    /// The header in the first page of the sequence.
    pub seq_header: Option<SeqHeader>,
    /// The ID of the next page in the sequence.
    pub next_page_id: Option<PageId>,
    /// Element count in this page.
    pub record_count: u16,
    /// Offset of the free bytes section.
    pub free_offset: u16,
}

impl Size for Header {
    fn size(&self) -> u32 {
        HeapPage::ty().size()
            + self.id.size()
            + self.seq_header.size()
            + self.next_page_id.size()
            + 2
            + 2
    }
}

impl Serialize for Header {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        HeapPage::ty().serialize(buf)?;
        self.id.serialize(buf)?;
        self.seq_header.serialize(buf)?;
        self.next_page_id.serialize(buf)?;
        buf.write(self.record_count);
        buf.write(self.free_offset);
        Ok(())
    }
}

impl Deserialize<'_> for Header {
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(Header {
            id: PageId::deserialize(buf)?,
            seq_header: Option::<SeqHeader>::deserialize(buf)?,
            next_page_id: Option::<PageId>::deserialize(buf)?,
            record_count: buf.read(),
            free_offset: buf.read(),
        })
    }
}

/// The [`HeapPage`] sequence header.
#[derive(Debug)]
pub struct SeqHeader {
    /// The ID of the last page in this sequence.
    pub last_page_id: PageId,
    /// The number of pages in this sequence.
    pub page_count: u32,
    /// The number of records in this sequence.
    pub record_count: u64,
}

impl Size for Option<SeqHeader> {
    fn size(&self) -> u32 {
        1 + self
            .as_ref()
            .map(|header| header.last_page_id.size() + 4 + 8)
            .unwrap_or(1)
    }
}

impl Serialize for Option<SeqHeader> {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        let Some(header) = self else {
            buf.write(0xAA_u8);
            return Ok(());
        };
        buf.write(0xFF_u8);
        header.last_page_id.serialize(buf)?;
        buf.write(header.page_count);
        buf.write(header.record_count);
        Ok(())
    }
}

impl Deserialize<'_> for Option<SeqHeader> {
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let discriminant: u8 = buf.read();
        match discriminant {
            0xAA => Ok(None),
            0xFF => Ok(Some(SeqHeader {
                last_page_id: PageId::deserialize(buf)?,
                page_count: buf.read(),
                record_count: buf.read(),
            })),
            unexpected => {
                error!(?unexpected, "invalid `SeqHeader` type discriminant");
                Err(Error::CorruptedTypeTag)
            }
        }
    }
}
