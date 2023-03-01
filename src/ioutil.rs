use std::num::NonZeroU32;

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

    /// Reads a variable-length blob, using a 2-byte length field.
    fn read_var_size_blob(&mut self) -> DbResult<Vec<u8>>;

    /// Writes a variable-length blob, using a 2-byte length field.
    fn write_var_size_blob(&mut self, blob: &[u8]) -> DbResult<()>;

    /// Reads a variable-length string, using a 2-byte length field.
    fn read_var_size_string(&mut self) -> DbResult<String>;

    /// Writes a variable-length string, using a 2-byte length field.
    fn write_var_size_string(&mut self, str: &str) -> DbResult<()>;
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

    fn read_var_size_blob(&mut self) -> DbResult<Vec<u8>> {
        let len: u16 = self.read();
        let mut buf = vec![0; len as usize]; // TODO: Optimize using `MaybeUninit`.
        self.read_slice(&mut buf);
        Ok(buf)
    }

    fn write_var_size_blob(&mut self, blob: &[u8]) -> DbResult<()> {
        self.write::<u16>(blob.len() as u16);
        self.write_slice(blob);
        Ok(())
    }

    fn read_var_size_string(&mut self) -> DbResult<String> {
        self.read_var_size_blob()
            .and_then(|bytes| String::from_utf8(bytes).map_err(|_| Error::CorruptedUtf8))
    }

    fn write_var_size_string(&mut self, str: &str) -> DbResult<()> {
        self.write_var_size_blob(str.as_bytes())
    }
}
