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
    /// Serializes the page.
    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()>;

    /// Deserializes the page.
    fn deserialize(buf: &mut Buff<'a>) -> DbResult<Self>
    where
        Self: Sized;
}

/// Defines methods specific to `fdb`'s implementation as extension methods in
/// the Buff type, which wouldn't make sense being defined there.
pub trait BuffExt {
    /// Reads `N` bytes and compares it to the given slice.
    fn read_verify_eq<const N: usize>(&mut self, expected: [u8; N]) -> Result<(), ()>;
}

impl BuffExt for Buff<'_> {
    fn read_verify_eq<const N: usize>(&mut self, expected: [u8; N]) -> Result<(), ()> {
        let mut actual = [0; N];
        self.read_slice(&mut actual);

        if actual == expected {
            Ok(())
        } else {
            Err(())
        }
    }
}

/// Serde wrapper for variable-length serialization format for byte strings.
pub struct VarBytes<'a>(pub Cow<'a, [u8]>);

impl<'a> Serde<'a> for VarBytes<'a> {
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
