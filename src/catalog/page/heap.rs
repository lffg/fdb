//! Heap pages store records in an unordered and sequential fashion.

use tracing::error;

use crate::{
    catalog::page::{Page, PageId},
    config::PAGE_SIZE,
    error::{DbResult, Error},
    ioutil::Serde,
};

/// The first [`HeapPage`] in the sequence.
#[derive(Debug)]
pub struct HeapPage {
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
    /// The record bytes in the page.
    pub bytes: Vec<u8>, // TODO: Review this.
}

impl Page for HeapPage {
    fn id(&self) -> PageId {
        self.id
    }
}

impl Serde for HeapPage {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.id.serialize(buf)?;
        self.seq_header.serialize(buf)?;
        self.next_page_id.serialize(buf)?;
        buf.write(self.record_count);
        buf.write(self.free_offset);
        buf.write_slice(&self.bytes);

        let rem = buf.remaining();
        if rem != 0 {
            buf.write_bytes(PAGE_SIZE as usize - rem, 0);
        }

        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(HeapPage {
            id: PageId::deserialize(buf)?,
            seq_header: Option::<SeqHeader>::deserialize(buf)?,
            next_page_id: Option::<PageId>::deserialize(buf)?,
            record_count: buf.read(),
            free_offset: buf.read(),
            bytes: {
                let mut bytes = vec![0; buf.remaining()];
                buf.read_slice(&mut bytes);
                bytes
            },
        })
    }
}

impl HeapPage {
    /// Constructs a new page.
    pub fn new(page_id: PageId) -> Self {
        HeapPage {
            id: page_id,
            seq_header: Some(SeqHeader {
                last_page_id: page_id,
                page_count: 1,
                record_count: 0,
            }),
            next_page_id: Some(page_id),
            record_count: 0,
            free_offset: 0,
            bytes: vec![],
        }
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

impl Serde for Option<SeqHeader> {
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
