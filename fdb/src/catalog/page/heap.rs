//! Heap pages store records in an unordered and sequential fashion.

use tracing::error;

use crate::{
    catalog::page::{Page, PageId, PageType, SpecificPage},
    config::PAGE_SIZE,
    error::{DbResult, Error},
    util::io::Serde,
};

/// The first [`HeapPage`] in the sequence.
#[derive(Debug)]
pub struct HeapPage {
    /// The page header.
    pub header: Header,
    /// The record bytes in the page.
    pub bytes: Vec<u8>, // TODO: Review this.
}

impl Serde<'_> for HeapPage {
    fn size(&self) -> u32 {
        self.header.size() + self.bytes.len() as u32
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.header.serialize(buf)?;
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

    fn default_with_id(page_id: PageId) -> Self {
        let header = Header {
            id: page_id,
            seq_header: Some(SeqHeader {
                last_page_id: page_id,
                page_count: 1,
                record_count: 0,
            }),
            next_page_id: Some(page_id),
            record_count: 0,
            free_offset: 0,
        };
        let bytes = vec![0; PAGE_SIZE as usize - header.size() as usize];

        Self { header, bytes }
    }

    super::impl_cast_methods!(Page::Heap => HeapPage);
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

impl Serde<'_> for Header {
    fn size(&self) -> u32 {
        HeapPage::ty().size()
            + self.id.size()
            + self.seq_header.size()
            + self.next_page_id.size()
            + 2
            + 2
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        HeapPage::ty().serialize(buf)?;
        self.id.serialize(buf)?;
        self.seq_header.serialize(buf)?;
        self.next_page_id.serialize(buf)?;
        buf.write(self.record_count);
        buf.write(self.free_offset);
        Ok(())
    }

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

impl Serde<'_> for Option<SeqHeader> {
    fn size(&self) -> u32 {
        1 + self
            .as_ref()
            .map(|header| header.last_page_id.size() + 4 + 8)
            .unwrap_or(1)
    }

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
