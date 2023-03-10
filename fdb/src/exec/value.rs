use std::{fmt, ops::Add};

use crate::{
    catalog::ty::{PrimitiveTypeId, TypeId},
    error::DbResult,
    util::io::{Serde, SerdeCtx, Size, VarBytes, VarString},
};

/// A database value.
#[derive(Clone, PartialEq, Eq)]
pub enum Value {
    Bool(bool),
    Byte(u8),
    ShortInt(i16),
    Int(i32),
    BigInt(i64),
    Timestamp(i64),
    Text(String),
    Blob(Vec<u8>),
    Array(PrimitiveTypeId, Vec<Value>), // TODO: Extract this as a type.
}

impl Size for Value {
    fn size(&self) -> u32 {
        match self {
            Value::Bool(_) => 1,
            Value::Byte(_) => 1,
            Value::ShortInt(_) => 2,
            Value::Int(_) => 4,
            Value::BigInt(_) => 8,
            Value::Timestamp(_) => 8,
            // 2-byte length and the string bytes (encoded in UTF-8).
            Value::Text(str) => 2 + u32::try_from(str.len()).unwrap(),
            // 2-byte length and the bytes.
            Value::Blob(bytes) => 2 + u32::try_from(bytes.len()).unwrap(),
            // 2-byte length and the elements.
            Value::Array(element_type, elements) => elements
                .iter()
                .map(|value| {
                    debug_assert_eq!(value.type_id().primitive_type_id(), *element_type);
                    value.size()
                })
                .sum::<u32>()
                .add(2), // length
        }
    }
}

impl SerdeCtx<'_, (), TypeId> for Value {
    fn serialize(&self, buf: &mut buff::Buff, _ctx: ()) -> DbResult<()> {
        match self {
            Value::Bool(inner) => buf.write(*inner),
            Value::Byte(inner) => buf.write(*inner),
            Value::ShortInt(inner) => buf.write(*inner),
            Value::Int(inner) => buf.write(*inner),
            Value::BigInt(inner) => buf.write(*inner),
            Value::Timestamp(inner) => buf.write(*inner),
            Value::Text(inner) => VarString::from(inner.as_str()).serialize(buf)?,
            Value::Blob(inner) => VarBytes::from(inner.as_slice()).serialize(buf)?,
            Value::Array(_element_type, elements) => {
                let len = elements.len() as u16;
                buf.write(len);
                for element in elements {
                    element.serialize(buf, ())?;
                }
            }
        }
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff, type_id: TypeId) -> DbResult<Self> {
        let value = match type_id {
            TypeId::Primitive(primitive_type) => match primitive_type {
                PrimitiveTypeId::Bool => Value::Bool(buf.read()),
                PrimitiveTypeId::Byte => Value::Byte(buf.read()),
                PrimitiveTypeId::ShortInt => Value::ShortInt(buf.read()),
                PrimitiveTypeId::Int => Value::Int(buf.read()),
                PrimitiveTypeId::BigInt => Value::BigInt(buf.read()),
                PrimitiveTypeId::Timestamp => Value::Timestamp(buf.read()),
                PrimitiveTypeId::Text => Value::Text(VarString::deserialize(buf)?.into()),
                PrimitiveTypeId::Blob => Value::Blob(VarBytes::deserialize(buf)?.into()),
            },
            TypeId::Array(element_type) => {
                let len: u16 = buf.read();
                let mut elements = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    elements.push(Value::deserialize(buf, TypeId::Primitive(element_type))?);
                }
                Value::Array(element_type, elements)
            }
        };
        Ok(value)
    }
}

impl Value {
    /// Returns the default value for the given [`TypeId`].
    pub fn default_for_type(ty: TypeId) -> Self {
        match ty {
            TypeId::Primitive(primitive) => match primitive {
                PrimitiveTypeId::Bool => Value::Bool(false),
                PrimitiveTypeId::Byte => Value::Byte(0),
                PrimitiveTypeId::ShortInt => Value::ShortInt(0),
                PrimitiveTypeId::Int => Value::Int(0),
                PrimitiveTypeId::BigInt => Value::BigInt(0),
                PrimitiveTypeId::Timestamp => Value::Timestamp(0),
                PrimitiveTypeId::Text => Value::Text(String::with_capacity(0)),
                PrimitiveTypeId::Blob => Value::Blob(Vec::with_capacity(0)),
            },
            TypeId::Array(element_type) => Value::Array(element_type, Vec::with_capacity(0)),
        }
    }

    /// Returns the corresponding type id.
    pub fn type_id(&self) -> TypeId {
        match self {
            Value::Bool(_) => TypeId::Primitive(PrimitiveTypeId::Bool),
            Value::Byte(_) => TypeId::Primitive(PrimitiveTypeId::Byte),
            Value::ShortInt(_) => TypeId::Primitive(PrimitiveTypeId::ShortInt),
            Value::Int(_) => TypeId::Primitive(PrimitiveTypeId::Int),
            Value::BigInt(_) => TypeId::Primitive(PrimitiveTypeId::BigInt),
            Value::Timestamp(_) => TypeId::Primitive(PrimitiveTypeId::Timestamp),
            Value::Text(_) => TypeId::Primitive(PrimitiveTypeId::Text),
            Value::Blob(_) => TypeId::Primitive(PrimitiveTypeId::Blob),
            Value::Array(element_type, _) => TypeId::Array(*element_type),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(inner) => inner.fmt(f),
            Value::Byte(inner) => inner.fmt(f),
            Value::ShortInt(inner) => inner.fmt(f),
            Value::Int(inner) => inner.fmt(f),
            Value::BigInt(inner) => inner.fmt(f),
            Value::Timestamp(inner) => inner.fmt(f),
            Value::Text(inner) => inner.fmt(f),
            Value::Blob(inner) => write!(f, "<bytes ({})>", inner.len()),
            Value::Array(element_type, elements) => {
                write!(f, "<array of {} ({})>", element_type.name(), elements.len())
            }
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(inner) => inner.fmt(f),
            Value::Byte(inner) => inner.fmt(f),
            Value::ShortInt(inner) => inner.fmt(f),
            Value::Int(inner) => inner.fmt(f),
            Value::BigInt(inner) => inner.fmt(f),
            Value::Timestamp(inner) => inner.fmt(f),
            Value::Text(_) => f.write_str("<string>"),
            Value::Blob(_) => f.write_str("<blob>"),
            Value::Array(element_type, _) => write!(f, "<array of {}>", element_type.name()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! t {
        ($name:ident, $expected_serialized:expr, $value:expr) => {
            #[test]
            fn $name() {
                const EXPECTED: &[u8] = $expected_serialized;
                const EXPECTED_SIZE: usize = EXPECTED.len();
                let value = $value;
                let ty = value.type_id();

                let mut buf = [0_u8; EXPECTED_SIZE];
                let buf = &mut buff::Buff::new(&mut buf);

                value.serialize(buf, ()).expect("serialize");
                assert_eq!(buf.get(), EXPECTED, "serialization didn't match");

                buf.seek(0);
                let deserialized_value = Value::deserialize(buf, ty).expect("deserialize");
                assert_eq!(deserialized_value, value, "deserialization didn't match");

                assert_eq!(value.size(), EXPECTED_SIZE as u32);
            }
        };
    }

    t!(bool, b"\x01", Value::Bool(true));

    t!(shortint, b"\x12\x34", Value::ShortInt(0x12_34));

    t!(int, b"\x12\x34\x56\x78", Value::Int(0x_1234_5678));

    t!(
        bigint,
        b"\x12\x34\x56\x78\x12\x34\x56\x78",
        Value::BigInt(0x_1234_5678_1234_5678)
    );

    t!(
        timestamp,
        b"\x12\x34\x56\x78\x12\x34\x56\x78",
        Value::Timestamp(0x_1234_5678_1234_5678)
    );

    t!(text, b"\x00\x05ol\xC3\xA1!", Value::Text("olá!".into()));

    t!(
        blob,
        b"\x00\x09ola-mundo",
        Value::Blob(b"ola-mundo".to_vec())
    );

    t!(
        array,
        b"\x00\x03\xAB\xCD\xEF",
        Value::Array(
            PrimitiveTypeId::Byte,
            vec![Value::Byte(0xAB), Value::Byte(0xCD), Value::Byte(0xEF)]
        )
    );
}
