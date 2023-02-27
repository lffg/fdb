use crate::{
    error::{DbResult, Error},
    ioutil::Serde,
};

/// `fdb` possible value types.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TypeId {
    Bool = 0,
    Byte = 1,
    ShortInt = 2,
    Int = 3,
    BigInt = 4,
    Timestamp = 5,
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
            5 => Ok(TypeId::Timestamp),
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

    /// Returns the canonical type name.
    pub fn name(&self) -> &'static str {
        match self {
            TypeId::Bool => "bool",
            TypeId::Byte => "byte",
            TypeId::ShortInt => "shortint",
            TypeId::Int => "int",
            TypeId::BigInt => "bigint",
            TypeId::Timestamp => "timestamp",
            TypeId::Text => "text",
            TypeId::Blob => "blob",
        }
    }
}
