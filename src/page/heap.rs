use crate::{
    error::{DbResult, Error},
    ioutil::{BuffExt, Serde},
    page::{schema_data::SchemaData, Page, PageId},
};

/// Heap page. Stores records in an unordered manner.
#[derive(Debug)]
pub struct HeapPage {
    pub id: PageId,
    pub next_page_id: Option<PageId>,
    pub ty: HeapPageType,
    pub bytes: Vec<u8>, // TODO: Review this.
}

impl Page for HeapPage {
    fn id(&self) -> PageId {
        self.id
    }
}

impl Serde for HeapPage {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write_page_id(Some(self.id));
        buf.write_page_id(self.next_page_id);
        self.ty.serialize(buf)?;
        buf.write_slice(&self.bytes);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let id = buf.read_page_id().expect("current page id");
        let next_page_id = buf.read_page_id();
        let ty = HeapPageType::deserialize(buf)?;

        let mut bytes = vec![0; buf.remaining()]; // TODO: Optimize using `MaybeUninit`.
        buf.read_slice(&mut bytes);

        Ok(HeapPage {
            id,
            next_page_id,
            ty,
            bytes,
        })
    }
}

/// Heap page type.
///
/// The first page of a heap page may store a schema associated with the next
/// heap pages in the "linked list".
#[derive(Debug)]
pub enum HeapPageType {
    FirstWithSchema(SchemaData),
    FirstWithoutSchema,
    Node,
}

impl Serde for HeapPageType {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.discriminant());
        if let HeapPageType::FirstWithSchema(schema) = self {
            schema.serialize(buf)?;
        }
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let tag: u8 = buf.read();
        match tag {
            0 => Ok(HeapPageType::FirstWithSchema(SchemaData::deserialize(buf)?)),
            1 => Ok(HeapPageType::FirstWithoutSchema),
            2 => Ok(HeapPageType::Node),
            _ => Err(Error::CorruptedHeapPageTypeTag),
        }
    }
}

impl HeapPageType {
    /// Returns the tag associated with the `HeapPageId`.
    pub const fn discriminant(&self) -> u8 {
        match self {
            HeapPageType::FirstWithSchema(_) => 0,
            HeapPageType::FirstWithoutSchema => 1,
            HeapPageType::Node => 2,
        }
    }
}
