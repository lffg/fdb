use std::{borrow::Cow, num::NonZeroU32};

use buff::Buff;

use crate::{
    catalog::page::PageId,
    error::{DbResult, Error},
};

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
pub trait Serde {
    /// Serializes the page.
    fn serialize(&self, buf: &mut Buff<'_>) -> DbResult<()>;

    /// Deserializes the page.
    // TODO: Maybe use an associated type to encode the error.
    fn deserialize(buf: &mut Buff<'_>) -> DbResult<Self>
    where
        Self: Sized;
}

/// Defines methods specific to `fdb`'s implementation as extension methods in
/// the Buff type, which wouldn't make sense being defined there.
pub trait BuffExt {
    /// Reads an optional page id. If the underlying data is 0, `None` is
    /// returned.
    fn read_page_id(&mut self) -> Option<PageId>;

    /// Writes an optional page id. If `None` is provided, 0 is written.
    fn write_page_id(&mut self, page: Option<PageId>);

    /// Reads `N` bytes and compares it to the given slice.
    fn read_verify_eq<const N: usize>(&mut self, expected: [u8; N]) -> Result<(), ()>;

    /// Reads a fixed-size UTF-8 string.
    fn read_fixed_size_string(
        &mut self,
        size: usize,
        value_name: impl Into<Cow<'static, str>>,
    ) -> DbResult<String>;

    /// Writes a fixed-size UTF-8 string.
    fn write_fixed_size_string(
        &mut self,
        size: usize,
        str: &str,
        value_name: impl Into<Cow<'static, str>>,
    ) -> DbResult<()>;
}

impl BuffExt for Buff<'_> {
    fn read_page_id(&mut self) -> Option<PageId> {
        let num = self.read();
        NonZeroU32::new(num).map(PageId::new)
    }

    fn write_page_id(&mut self, page: Option<PageId>) {
        let num = page.map(PageId::get).unwrap_or(0);
        self.write(num);
    }

    fn read_verify_eq<const N: usize>(&mut self, expected: [u8; N]) -> Result<(), ()> {
        let mut actual = [0; N];
        self.read_slice(&mut actual);

        if actual == expected {
            Ok(())
        } else {
            Err(())
        }
    }

    fn read_fixed_size_string(
        &mut self,
        size: usize,
        value_name: impl Into<Cow<'static, str>>,
    ) -> DbResult<String> {
        let mut buf = vec![0; size];
        self.read_slice(&mut buf);

        let hi = buf.iter().position(|byte| *byte == 0).unwrap_or(buf.len());
        buf.truncate(hi);

        let string = String::from_utf8(buf).map_err(|_| Error::CorruptedUtf8(value_name.into()))?;
        Ok(string)
    }

    fn write_fixed_size_string(
        &mut self,
        size: usize,
        str: &str,
        value_name: impl Into<Cow<'static, str>>,
    ) -> DbResult<()> {
        if str.len() > size {
            return Err(Error::SizeGreaterThanExpected {
                name: value_name.into(),
                expected: size,
                actual: str.len(),
            });
        }
        self.scoped_exact(size, |buf| {
            let bytes = str.as_bytes();
            buf.write_slice(bytes);
            buf.write_bytes(size - bytes.len(), 0);
        });
        Ok(())
    }
}
