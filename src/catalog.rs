use crate::{
    config::IDENTIFIER_SIZE,
    error::{DbResult, Error},
    ioutil::{BuffExt, Serde},
    page::PageId,
};

/// `fdb` possible value types.
#[derive(Copy, Clone, Debug)]
pub enum TypeId {
    Bool = 0,
    Byte = 1,
    ShortInt = 2,
    Int = 3,
    BigInt = 4,
    DateTime = 5,
    Text = 6,
    Blob = 7,
}

impl Serde for TypeId {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.discriminant());
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let tag: u8 = buf.read();
        match tag {
            0 => Ok(TypeId::Bool),
            1 => Ok(TypeId::Byte),
            2 => Ok(TypeId::ShortInt),
            3 => Ok(TypeId::Int),
            4 => Ok(TypeId::BigInt),
            5 => Ok(TypeId::DateTime),
            6 => Ok(TypeId::Text),
            7 => Ok(TypeId::Blob),
            _ => Err(Error::CorruptedTypeTag),
        }
    }
}

impl TypeId {
    /// Returns the tag associated with the `HeapPageId`.
    pub const fn discriminant(&self) -> u8 {
        *self as u8
    }

    /// Returns the size (in bytes) for the given type.
    pub const fn _size(self) -> u8 {
        match self {
            TypeId::Bool | TypeId::Byte => 1,
            TypeId::ShortInt => 2,
            TypeId::Int => 4,
            TypeId::BigInt => 8,
            TypeId::DateTime => panic!("todo(lffg): decide on DateTime representation"),
            TypeId::Text | TypeId::Blob => 16,
        }
    }
}

/// A column definition.
#[derive(Debug)]
pub struct Column {
    /// The column value type.
    pub ty: TypeId,
    /// The column identifier.
    ///
    /// The column name may have at most 64 bytes.
    pub name: String,
}

impl Serde for Column {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.ty.serialize(buf)?;
        buf.write_fixed_size_string(IDENTIFIER_SIZE, &self.name, "column name")?;
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(Column {
            ty: TypeId::deserialize(buf)?,
            name: buf.read_fixed_size_string(IDENTIFIER_SIZE, "column name")?,
        })
    }
}

/// A database's object definition, which contains high-level information that
/// describes the database object.
#[derive(Debug)]
pub struct Object {
    /// The object's type (e.g. a table, an index, etc).
    pub ty: ObjectType,
    /// The ID of the first page that stores the actual records.
    pub page_id: PageId,
    /// The object name (e.g. the table name as per the user's definition).
    ///
    /// The object name (i.e., a table name or an index name) may have at most
    /// 64 bytes.
    pub name: String,
}

impl Serde for Object {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.ty.serialize(buf)?;
        buf.write_page_id(Some(self.page_id));
        buf.write_fixed_size_string(IDENTIFIER_SIZE, &self.name, self.ty.name())?;
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let ty = ObjectType::deserialize(buf)?;
        Ok(Object {
            ty,
            page_id: buf.read_page_id().expect("non-null page id"),
            name: buf.read_fixed_size_string(IDENTIFIER_SIZE, ty.name())?,
        })
    }
}

/// An [`Object`] type.
#[derive(Debug, Copy, Clone)]
pub enum ObjectType {
    Table = 0xA,
    Index = 0xB,
}

impl Serde for ObjectType {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.discriminant());
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let tag: u8 = buf.read();
        match tag {
            0xA => Ok(ObjectType::Table),
            0xB => Ok(ObjectType::Index),
            _ => Err(Error::CorruptedObjectTypeTag),
        }
    }
}

impl ObjectType {
    /// Returns the tag associated with the `HeapPageId`.
    pub const fn discriminant(&self) -> u8 {
        *self as u8
    }

    /// Returns the name of the object type.
    pub const fn name(&self) -> &'static str {
        match self {
            ObjectType::Table => "table",
            ObjectType::Index => "index",
        }
    }
}
