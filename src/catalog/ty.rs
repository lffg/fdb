use crate::{
    error::{DbResult, Error},
    ioutil::Serde,
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
