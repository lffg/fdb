use crate::{
    catalog::{
        column::Column,
        page::{Page, PageId},
    },
    error::{DbResult, Error},
    ioutil::{BuffExt, Serde},
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
    FirstWithSchema(TableSchema),
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
            0 => Ok(HeapPageType::FirstWithSchema(TableSchema::deserialize(
                buf,
            )?)),
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

#[derive(Debug)]
pub struct TableSchema {
    pub column_count: u16,
    pub columns: Vec<Column>,
}

impl Serde for TableSchema {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.column_count);
        debug_assert_eq!(self.column_count as usize, self.columns.len());
        for column in &self.columns {
            column.serialize(buf)?;
        }
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let column_count: u16 = buf.read();
        let columns: Vec<_> = (0..column_count)
            .map(|_| Column::deserialize(buf))
            .collect::<Result<_, _>>()?;
        Ok(TableSchema {
            column_count,
            columns,
        })
    }
}
