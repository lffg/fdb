use tracing::error;

use crate::{
    error::{DbResult, Error},
    util::io::Serde,
};

/// `fdb` possible value types.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeId {
    /// A primitive (i.e., non-composite) type.
    Primitive(PrimitiveTypeId),
    /// Single-dimension array type. Serialized using bitwise operations.
    ///
    ///   0001<type_id>
    ///   |  |\-------/-------> 4 bits to encode the element type.
    ///   |  |
    ///   \--/-----------> 4 bits to encode the "array type".
    ///
    /// For example, assuming that `2` (0010) encodes the short integer type, to
    /// represent an array of short integers, one would use `0001_0010`. To
    /// represent a simple (i.e., primitive) short integer, `0000_0010`.
    Array(PrimitiveTypeId),
}

impl Serde<'_> for TypeId {
    fn size(&self) -> u32 {
        1
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        let (hi_discriminant, lo_discriminant) = match self {
            TypeId::Primitive(primitive) => (0, primitive.to_u8()),
            TypeId::Array(primitive) => (1, primitive.to_u8()),
        };
        // Those parentheses are necessary. <:
        let discriminant: u8 = (hi_discriminant << 4) + lo_discriminant;
        buf.write(discriminant);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let tag: u8 = buf.read();

        let hi_discriminant = tag >> 4; // 4 most significant bits
        let lo_discriminant = tag & 0xF; // 4 least significant bits

        let primitive_type = PrimitiveTypeId::try_from_u8(lo_discriminant)?;

        match hi_discriminant {
            0 => Ok(Self::Primitive(primitive_type)),
            1 => Ok(Self::Array(primitive_type)),
            unexpected => {
                error!(?unexpected, "invalid `TypeId` type discriminant");
                Err(Error::CorruptedTypeTag)
            }
        }
    }
}

impl TypeId {
    /// Returns the canonical type name.
    pub fn name(self) -> &'static str {
        match self {
            TypeId::Primitive(primitive) => primitive.name(),
            TypeId::Array(_) => "array",
        }
    }

    /// Fetches the underlying primitive type ID.
    ///
    /// Panics if the actual type is not a primitive.
    pub fn primitive_type_id(self) -> PrimitiveTypeId {
        match self {
            TypeId::Primitive(primitive) => primitive,
            other => panic!("`{}` is not a primitive type", other.name()),
        }
    }
}

/// `fdb` possible primitive (i.e., non-composite) value types.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PrimitiveTypeId {
    Bool = 0,
    Byte = 1,
    ShortInt = 2,
    Int = 3,
    BigInt = 4,
    Timestamp = 5,
    Text = 6,
    Blob = 7,
}

impl Serde<'_> for PrimitiveTypeId {
    fn size(&self) -> u32 {
        1
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.to_u8());
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Self::try_from_u8(buf.read())
    }
}

impl PrimitiveTypeId {
    /// Returns the canonical type name.
    pub fn name(self) -> &'static str {
        match self {
            PrimitiveTypeId::Bool => "bool",
            PrimitiveTypeId::Byte => "byte",
            PrimitiveTypeId::ShortInt => "shortint",
            PrimitiveTypeId::Int => "int",
            PrimitiveTypeId::BigInt => "bigint",
            PrimitiveTypeId::Timestamp => "timestamp",
            PrimitiveTypeId::Text => "text",
            PrimitiveTypeId::Blob => "blob",
        }
    }

    /// Serialized representation.
    fn to_u8(self) -> u8 {
        self as u8
    }

    /// Deserialize the type id from the given byte.
    fn try_from_u8(serialized: u8) -> DbResult<Self> {
        match serialized {
            0 => Ok(PrimitiveTypeId::Bool),
            1 => Ok(PrimitiveTypeId::Byte),
            2 => Ok(PrimitiveTypeId::ShortInt),
            3 => Ok(PrimitiveTypeId::Int),
            4 => Ok(PrimitiveTypeId::BigInt),
            5 => Ok(PrimitiveTypeId::Timestamp),
            6 => Ok(PrimitiveTypeId::Text),
            7 => Ok(PrimitiveTypeId::Blob),
            unexpected => {
                error!(?unexpected, "invalid `PrimitiveTypeId` type discriminant");
                Err(Error::CorruptedTypeTag)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_id_representation() {
        const CASES: &[(u8, TypeId)] = &[
            (0b0001_0010, TypeId::Array(PrimitiveTypeId::ShortInt)),
            (0b0000_0010, TypeId::Primitive(PrimitiveTypeId::ShortInt)),
            (0b0001_0110, TypeId::Array(PrimitiveTypeId::Text)),
            (0b0000_0110, TypeId::Primitive(PrimitiveTypeId::Text)),
        ];

        let mut buf = [0_u8; 1];
        let buf = &mut buff::Buff::new(&mut buf);

        for case @ &(repr, type_id) in CASES {
            buf.seek(0);
            type_id.serialize(buf).expect("should serialize");
            assert_eq!(
                buf.get()[0],
                repr,
                "invalid serialization for case `{case:?}`"
            );

            buf.seek(0);
            let deserialized_type_id = TypeId::deserialize(buf).expect("should deserialize");
            assert_eq!(
                deserialized_type_id, type_id,
                "invalid deserialization for case `{case:?}`"
            );
        }
    }
}
