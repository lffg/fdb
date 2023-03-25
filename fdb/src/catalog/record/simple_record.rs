use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::{self, Debug},
    ops::Add,
};

use crate::{
    catalog::{page::PageId, table_schema::TableSchema},
    error::DbResult,
    util::io::{Serde, SerdeCtx, Size},
};

/// A simple database record. May store arbitrary bytes which are to be
/// interpreted in a higher-level layer.
pub struct SimpleRecord<'d, D>
where
    D: Clone,
{
    /// The [`PageId`] of the page on which this record is stored.
    ///
    /// This value is not serialized.
    page_id: PageId,
    /// The offset of the record in the table.
    ///
    /// This value is not serialized.
    offset: u16,
    /// The record's total size.
    total_size: u16,
    /// Whether the record is logically deleted.
    is_deleted: bool,
    /// The record's bytes. Notice that the size of this section is stored as a
    /// 2-byte number.
    // TODO(buff-trait): Use a slice here.
    data: Cow<'d, D>,
    /// The size of the padding section.
    ///
    /// Though the database just stores zeroes at the end (without an explicit
    /// size), the in-memory record representation doesn't need the padding.
    /// Hence, one just stores the padding section's size here.
    pad_size: u16,
}

impl<'d, D> SimpleRecord<'d, D>
where
    D: Size + Clone,
{
    /// Constructs a new record.
    pub fn new(page_id: PageId, offset: u16, data: Cow<'d, D>) -> SimpleRecord<'d, D> {
        let mut record = SimpleRecord {
            page_id,
            offset,
            total_size: 0, // <---- One updates this below.
            is_deleted: false,
            data,
            pad_size: 0,
        };
        record.total_size = record.size() as u16;
        record
    }

    /// Checks whether the record is deleted.
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    /// Marks the record as being deleted.
    pub fn set_deleted(&mut self) {
        self.is_deleted = true;
    }

    /// Returns the record's [`PageId`].
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Returns the record's offset.
    pub fn offset(&self) -> u16 {
        self.offset
    }

    /// Returns a reference to the underlying data.
    pub fn as_data(&'d self) -> &'d D {
        &self.data
    }

    /// Returns the inner record data.
    pub fn into_data(self) -> Cow<'d, D> {
        self.data
    }

    /// Tries to update the record with the given `new_data`. Three cases may
    /// occur:
    ///
    /// 1. The size of `new_data` is the same as the previous one. In such a
    ///    case, nothing special happens.
    /// 2. The size of `new_data` is **smaller** than the previous one. In such
    ///    a case, the size difference is incremented to `self`'s `pad_count`
    ///    field.
    /// 3. The size of `new_data` is **greater** than the previous one. In such
    ///    a case, no modifications are made in the given record. Callers should
    ///    also call `set_deleted`, for example.
    ///
    /// In all cases, `self` is mutated in place. For case `3`, `new_data` is
    /// returned in the result's error variant.
    ///
    /// Notice that updates don't change the current record's `total_size`.
    pub fn try_update(&mut self, new_data: Cow<'d, D>) -> Result<(), Cow<'d, D>> {
        let total_size = self.available_data_size();
        let new_size = new_data.size();

        match new_size.cmp(&total_size) {
            Ordering::Less => {
                self.pad_size += (total_size - new_size) as u16;
                self.data = new_data;
                Ok(())
            }
            Ordering::Equal => {
                self.pad_size = 0;
                self.data = new_data;
                Ok(())
            }
            Ordering::Greater => Err(new_data),
        }
    }

    /// Returns the available size for the `data` section.
    fn available_data_size(&self) -> u32 {
        self.size() - 2 - 1
    }
}

impl<D> Size for SimpleRecord<'_, D>
where
    D: Size + Clone,
{
    fn size(&self) -> u32 {
        (2_u32) // total size
            .add(1) // is deleted flag
            .add(self.data.size()) // data
            .add(self.pad_size as u32) // padding size
    }
}

/// Serde implementation for table's data records.
impl<D> SerdeCtx<'_, TableRecordCtx<'_>, TableRecordCtx<'_>> for SimpleRecord<'_, D>
where
    D: for<'a> SerdeCtx<'a, TableSchema, TableSchema> + Clone,
{
    fn serialize(&self, buf: &mut buff::Buff<'_>, ctx: &TableRecordCtx<'_>) -> DbResult<()> {
        buf.write(self.total_size);
        buf.write(self.is_deleted);
        self.data.serialize(buf, &ctx.schema)?;
        buf.write_bytes(self.pad_size as usize, 0);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>, ctx: &TableRecordCtx<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let total_size: u16 = buf.read();
        let is_deleted: bool = buf.read();
        let data = D::deserialize(buf, &ctx.schema)?;

        let pad_size = total_size - 2 - 1 - data.size() as u16;

        if cfg!(debug_assertions) {
            // Ensure one is reading zeroes in debug mode.
            for _ in 0..pad_size {
                let byte: u8 = buf.read();
                debug_assert_eq!(byte, 0);
            }
        } else {
            buf.seek_advance(pad_size as usize);
        }

        Ok(SimpleRecord {
            page_id: ctx.page_id,
            offset: ctx.offset,
            total_size,
            is_deleted,
            data: Cow::Owned(data),
            pad_size,
        })
    }
}

/// Serde implementation for general [`Serde`] types.
impl<D> SerdeCtx<'_, (), SimpleCtx> for SimpleRecord<'_, D>
where
    D: for<'a> Serde<'a> + Clone,
{
    fn serialize(&self, buf: &mut buff::Buff<'_>, _ctx: &()) -> DbResult<()> {
        buf.write(self.total_size);
        buf.write(self.is_deleted);
        self.data.serialize(buf)?;
        buf.write_bytes(self.pad_size as usize, 0);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>, ctx: &SimpleCtx) -> DbResult<Self>
    where
        Self: Sized,
    {
        let total_size: u16 = buf.read();
        let is_deleted: bool = buf.read();
        let data = D::deserialize(buf)?;

        let pad_size = total_size - 2 - 1 - data.size() as u16;

        if cfg!(debug_assertions) {
            // Ensure one is reading zeroes in debug mode.
            for _ in 0..pad_size {
                let byte: u8 = buf.read();
                debug_assert_eq!(byte, 0);
            }
        } else {
            buf.seek_advance(pad_size as usize);
        }

        Ok(SimpleRecord {
            page_id: ctx.page_id,
            offset: ctx.offset,
            total_size,
            is_deleted,
            data: Cow::Owned(data),
            pad_size,
        })
    }
}

pub struct SimpleCtx {
    /// The [`PageId`].
    pub page_id: PageId,
    /// The starting offset of the record.
    ///
    /// Notice that this *may* not be the *actual* page offset. It *may* be an
    /// "opaque offset".
    pub offset: u16,
}

pub struct TableRecordCtx<'a> {
    /// The [`PageId`].
    pub page_id: PageId,
    /// The table schema associated with the record.
    pub schema: &'a TableSchema,
    /// The starting offset of the record. For more info, see [`OffsetCtx`].
    pub offset: u16,
}

impl<D> Debug for SimpleRecord<'_, D>
where
    D: Clone + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleRecord")
            .field("offset", &self.offset)
            .field("total_size", &self.total_size)
            .field("is_deleted", &self.is_deleted)
            .field("data", &self.data)
            .field("pad_size", &self.pad_size)
            .finish()
    }
}
