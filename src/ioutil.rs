use std::num::NonZeroU32;

use buff::Buff;

use crate::{error::DbResult, page::PageId};

/// Defines a common serialization/deserialization interface based in the
/// [`Buff`] type.
///
/// Besides the name inspiration, this has nothing to do with the
/// [serde](https://serde.rs) crate. :P
pub trait Serde {
    /// Serializes the page.
    ///
    /// This operation should be infallible.
    fn serialize(&self, buf: &mut Buff<'_>);

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
}
