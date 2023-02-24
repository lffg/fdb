use crate::page::PageId;

/// `fdb` possible value types.
#[derive(Copy, Clone, Debug)]
pub enum TypeId {
    Bool,
    Byte,
    ShortInt,
    Int,
    BigInt,
    DateTime,
    Text,
    Blob,
}

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

/// A column definition.
#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub ty: TypeId,
}

/// The in-memory database catalog, which contains
#[derive(Debug)]
pub struct Catalog {
    entries: Vec<Catalog>,
}

/// A database's object definition, which contains high-level information that
/// describes the database object.
#[derive(Debug)]
pub struct Object {
    /// The object's type (e.g. a table, an index, etc).
    ty: ObjectType,
    /// The ID of the first page that stores the actual records.
    page: PageId,
    /// The object name (e.g. the table name as per the user's definition).
    name: String,
}

/// An [`Object`] type.
#[derive(Debug, Copy, Clone)]
pub enum ObjectType {
    Table = 0xA,
    Index = 0xB,
}
