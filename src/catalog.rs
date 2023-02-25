use crate::{
    error::{DbResult, Error},
    ioutil::{BuffExt, Serde},
    page::PageId,
};

/// `fdb` possible value types.
#[derive(Copy, Clone, Debug)]
pub enum TypeId {
    _Bool,
    _Byte,
    _ShortInt,
    _Int,
    _BigInt,
    _DateTime,
    _Text,
    _Blob,
}

/*
impl TypeId {
    /// Returns the size (in bytes) for the given type.
    pub const fn size(self) -> u8 {
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
*/

/// A column definition.
#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub ty: TypeId,
}

/// A database's object definition, which contains high-level information that
/// describes the database object.
#[derive(Debug)]
pub struct Object {
    /// The object's type (e.g. a table, an index, etc).
    pub ty: ObjectType,
    /// The ID of the first page that stores the actual records.
    pub page: PageId,
    /// The object name (e.g. the table name as per the user's definition).
    ///
    /// The object name (i.e., a table name or an index name) may have at most
    /// 64 bytes.
    pub name: String,
}

impl Serde for Object {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        let Object { ty, page, name } = self;
        ty.serialize(buf)?;
        buf.write_page_id(Some(*page));
        buf.write_fixed_size_string(6, name, ty.name())?;
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let ty = ObjectType::deserialize(buf)?;
        Ok(Object {
            ty,
            page: buf.read_page_id().expect("non-null page id"),
            name: buf.read_fixed_size_string(64, ty.name())?,
        })
    }
}

/// An [`Object`] type.
#[derive(Debug, Copy, Clone)]
pub enum ObjectType {
    Table = 0xA,
    Index = 0xB,
}

impl ObjectType {
    /// Returns the name of the object type.
    pub const fn name(&self) -> &'static str {
        match self {
            ObjectType::Table => "table",
            ObjectType::Index => "index",
        }
    }
}

impl Serde for ObjectType {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        let repr = *self as u8;
        buf.write(repr);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let repr: u8 = buf.read();
        match repr {
            0xA => Ok(ObjectType::Table),
            0xB => Ok(ObjectType::Index),
            _ => Err(Error::CorruptedObjectType),
        }
    }
}
