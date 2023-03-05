use std::borrow::Cow;

use buff::Buff;

use crate::error::{DbResult, Error};

/// Defines a common serialization/deserialization interface based in the
/// [`Buff`] type.
///
/// During serialization, it is the caller's responsibility to ensure that the
/// inner page has the capacity to store the object being serialized. If this
/// contract is not upheld, `Buff`'s implementation will panic once the buffer
/// (of `PAGE_SIZE` length) is full.
///
/// Besides the name inspiration, this has nothing to do with the
/// [serde](https://serde.rs) crate. :P
pub trait Serde<'a> {
    /// Returns the size of the serialized representation.
    fn size(&self) -> u32;

    /// Serializes the page.
    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()>;

    /// Deserializes the page.
    fn deserialize(buf: &mut Buff<'a>) -> DbResult<Self>
    where
        Self: Sized;
}

/// Asserts that the next `expected.len()` bytes are equal to `expected`.
///
/// Returns `true` is the read string was correctly verified.
pub fn read_verify_eq(buf: &mut Buff<'_>, expected: &[u8]) -> bool {
    expected.iter().all(|byte| *byte == buf.read::<1, u8>())
}

/// Serde wrapper for a variable-length record list.
pub struct VarList<'a, T>(pub Cow<'a, [T]>)
where
    [T]: ToOwned;

impl<'a, T> Serde<'a> for VarList<'a, T>
where
    [T]: ToOwned,
    &'a [T]: IntoIterator,
    <[T] as ToOwned>::Owned: FromIterator<T>,
    T: for<'b> Serde<'b>,
{
    fn size(&self) -> u32 {
        2 + self.0.iter().map(Serde::size).sum::<u32>()
    }

    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()> {
        let len = u16::try_from(self.0.len()).expect("u16 length");
        buf.write(len);
        for item in self.0.iter() {
            item.serialize(buf)?;
        }
        Ok(())
    }

    fn deserialize(buf: &mut Buff<'a>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let len: u16 = buf.read();
        let inner = (0..len)
            .map(|_| T::deserialize(buf))
            .collect::<Result<_, _>>()?;
        Ok(VarList(Cow::Owned(inner)))
    }
}

impl<'a, T> From<&'a [T]> for VarList<'a, T>
where
    [T]: ToOwned,
{
    fn from(value: &'a [T]) -> Self {
        VarList(Cow::Borrowed(value))
    }
}

impl<T> From<Vec<T>> for VarList<'_, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn from(value: Vec<T>) -> Self {
        VarList(Cow::Owned(value))
    }
}

impl<'a, T> From<&'a VarList<'_, T>> for &'a [T]
where
    [T]: ToOwned,
{
    fn from(value: &'a VarList<'_, T>) -> Self {
        &value.0
    }
}

impl<'a, T> From<VarList<'a, T>> for Vec<T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn from(value: VarList<'a, T>) -> Self {
        value.0.into_owned()
    }
}

/// Serde wrapper for variable-length serialization format for byte strings.
pub struct VarBytes<'a>(pub Cow<'a, [u8]>);

impl<'a> Serde<'a> for VarBytes<'a> {
    fn size(&self) -> u32 {
        2 + self.0.len() as u32
    }

    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()> {
        buf.write::<u16>(self.0.len() as u16);
        buf.write_slice(&self.0);
        Ok(())
    }

    fn deserialize(buf: &mut Buff<'_>) -> DbResult<VarBytes<'a>>
    where
        Self: Sized,
    {
        let len: u16 = buf.read();
        let mut bytes = vec![0; len as usize]; // TODO: Optimize using `MaybeUninit`.
        buf.read_slice(&mut bytes);
        Ok(VarBytes(Cow::Owned(bytes)))
    }
}

/// [`Serde`] wrapper for variable-length serialization format for strings.
pub struct VarString<'a>(pub Cow<'a, str>);

impl<'a> Serde<'a> for VarString<'a> {
    fn size(&self) -> u32 {
        2 + self.0.len() as u32
    }

    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()> {
        VarBytes(Cow::Borrowed(self.0.as_bytes())).serialize(buf)
    }

    fn deserialize(buf: &mut Buff<'a>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let bytes = VarBytes::deserialize(buf)?.0.into_owned();
        let string = String::from_utf8(bytes).map_err(|_| Error::CorruptedUtf8)?;
        Ok(VarString(Cow::Owned(string)))
    }
}

macro_rules! impl_from_var {
    ($borrowed:ty, $owned:ty => $var:ident) => {
        impl<'a> From<&'a $borrowed> for $var<'a> {
            fn from(value: &'a $borrowed) -> Self {
                $var(Cow::Borrowed(value))
            }
        }

        impl From<$owned> for $var<'_> {
            fn from(value: $owned) -> Self {
                $var(Cow::Owned(value))
            }
        }

        impl From<$var<'_>> for $owned {
            fn from(value: $var<'_>) -> Self {
                value.0.into_owned()
            }
        }

        impl<'a> From<&'a $var<'_>> for &'a $borrowed {
            fn from(value: &'a $var<'_>) -> Self {
                &value.0
            }
        }
    };
}

impl_from_var!([u8], Vec<u8> => VarBytes);
impl_from_var!(str, String => VarString);
