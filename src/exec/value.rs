use std::{collections::HashMap, fmt};

use crate::{
    catalog::ty::TypeId,
    error::DbResult,
    ioutil::{Serde, VarBytes, VarString},
};

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

impl Value {
    /// Returns the size of the serialized byte stream.
    pub fn size(&self) -> u32 {
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

    /// Serializes the given value into `buf`.
    pub fn serialize(&self, buf: &mut buff::Buff) -> DbResult<()> {
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

    /// Deserializes the value of the given type id from the given `buf`.
    pub fn deserialize(type_id: TypeId, buf: &mut buff::Buff) -> DbResult<Self> {
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

/// An environment that map from column names to values.
#[derive(Debug, Default)]
pub struct Environment {
    inner: HashMap<String, Value>,
}

impl Environment {
    /// Returns the total size of the environment.
    pub fn size(&self) -> u32 {
        self.inner
            .values()
            .map(Value::size)
            .fold(0, std::ops::Add::add)
    }

    /// Returns a reference to the underlying value.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.inner.get(name)
    }

    /// Sets a value.
    pub fn set(&mut self, name: String, value: Value) {
        self.inner.insert(name, value);
    }
}

impl From<HashMap<String, Value>> for Environment {
    fn from(inner: HashMap<String, Value>) -> Self {
        Environment { inner }
    }
}
