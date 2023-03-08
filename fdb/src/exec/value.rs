use std::fmt;

use crate::{
    catalog::ty::TypeId,
    error::DbResult,
    util::io::{Serde, SerdeCtx, VarBytes, VarString},
};

/// A database value.
#[derive(Clone)]
pub enum Value {
    Bool(bool),
    Byte(u8),
    ShortInt(i16),
    Int(i32),
    BigInt(i64),
    Timestamp(i64),
    Text(String),
    Blob(Vec<u8>),
}

impl SerdeCtx<'_> for Value {
    type SerCtx<'ser> = ();

    type DeCtx<'de> = TypeId;

    fn size(&self) -> u32 {
        match self {
            Value::Bool(_) => 1,
            Value::Byte(_) => 1,
            Value::ShortInt(_) => 2,
            Value::Int(_) => 4,
            Value::BigInt(_) => 8,
            Value::Timestamp(_) => 8,
            // 2-byte length.
            Value::Text(str) => 2 + u32::try_from(str.len()).unwrap(),
            // 2-byte length.
            Value::Blob(bytes) => 2 + u32::try_from(bytes.len()).unwrap(),
        }
    }

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
        }
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff, type_id: TypeId) -> DbResult<Self> {
        let value = match type_id {
            TypeId::Bool => Value::Bool(buf.read()),
            TypeId::Byte => Value::Byte(buf.read()),
            TypeId::ShortInt => Value::ShortInt(buf.read()),
            TypeId::Int => Value::Int(buf.read()),
            TypeId::BigInt => Value::BigInt(buf.read()),
            TypeId::Timestamp => Value::Timestamp(buf.read()),
            TypeId::Text => Value::Text(VarString::deserialize(buf)?.into()),
            TypeId::Blob => Value::Blob(VarBytes::deserialize(buf)?.into()),
        };
        Ok(value)
    }
}

impl Value {
    /// Returns the default value for the given [`TypeId`].
    pub fn default_for_type(ty: TypeId) -> Self {
        match ty {
            TypeId::Bool => Value::Bool(false),
            TypeId::Byte => Value::Byte(0),
            TypeId::ShortInt => Value::ShortInt(0),
            TypeId::Int => Value::Int(0),
            TypeId::BigInt => Value::BigInt(0),
            TypeId::Timestamp => Value::Timestamp(0),
            TypeId::Text => Value::Text(String::with_capacity(0)),
            TypeId::Blob => Value::Blob(Vec::with_capacity(0)),
        }
    }

    /// Returns the corresponding type id.
    pub fn type_id(&self) -> TypeId {
        match self {
            Value::Bool(_) => TypeId::Bool,
            Value::Byte(_) => TypeId::Byte,
            Value::ShortInt(_) => TypeId::ShortInt,
            Value::Int(_) => TypeId::Int,
            Value::BigInt(_) => TypeId::BigInt,
            Value::Timestamp(_) => TypeId::Timestamp,
            Value::Text(_) => TypeId::Text,
            Value::Blob(_) => TypeId::Blob,
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
        }
    }
}
