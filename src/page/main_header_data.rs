use crate::{
    error::{DbResult, Error},
    ioutil::{BuffExt, Serde},
    page::PageId,
};

pub const HEADER_SIZE: usize = 100;

/// The database header.
#[derive(Debug)]
pub struct MainHeaderData {
    /// The file format version. Currently, such a version is defined as `0`.
    pub file_format_version: u8,
    /// The total number of pages being used in the file.
    pub page_count: u32,
    /// The ID of the first free list page.
    pub first_free_list_page_id: Option<PageId>,
}

impl Serde for MainHeaderData {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.scoped_exact(HEADER_SIZE, |buf| {
            buf.write_slice(b"fdb format");
            buf.write(self.file_format_version);
            buf.write(self.page_count);
            buf.write_page_id(self.first_free_list_page_id);

            let rest = HEADER_SIZE - 2 - buf.offset();
            buf.write_bytes(rest, 0);
            buf.write_slice(br"\0");
        });
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        buf.scoped_exact(HEADER_SIZE, |buf| {
            buf.read_verify_eq::<10>(*b"fdb format")
                .map_err(|_| Error::CorruptedHeader("start"))?; // header sig

            let header = MainHeaderData {
                file_format_version: buf.read(),
                page_count: buf.read(),
                first_free_list_page_id: buf.read_page_id(),
            };

            buf.seek(HEADER_SIZE - 2);
            buf.read_verify_eq::<2>(*br"\0")
                .map_err(|_| Error::CorruptedHeader("end"))?; // finish header sig

            Ok(header)
        })
    }
}
