use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use bytes::{Bytes, BytesMut};

use crate::{
    config::PAGE_SIZE,
    error::{DbResult, Error},
    page::PageId,
};

pub struct DiskManager {
    file: File,
}

impl DiskManager {
    /// Opens the file at the provided path and constructs a new disk manager
    /// instance that wraps over it.
    pub fn new(path: &Path) -> DbResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // TODO: Add `O_DIRECT` flag.
            .open(path)?;

        Ok(DiskManager { file })
    }

    /// Reads the contents of the page at the offset from the given page id,
    /// writing them at the provided buffer.
    ///
    /// # Errors
    ///
    /// - If the offset provided by the given page id exceeds the inner file's
    ///   length.
    /// - If one of the I/O operations fails.
    ///
    /// # Panics
    ///
    /// - If `buf`'s length is different than [`PAGE_SIZE`].
    pub fn read_page(&mut self, page_id: PageId, buf: &mut BytesMut) -> DbResult<()> {
        assert_eq!(buf.len() as u64, PAGE_SIZE);

        let size = self.file.metadata()?.len();
        let offset = page_id.offset();
        if offset >= size {
            return Err(Error::PageOutOfBounds(page_id));
        }

        self.file.seek(SeekFrom::Start(page_id.offset()))?;

        if let Err(error) = self.file.read_exact(&mut buf[..]) {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                Err(Error::ReadIncompletePage(page_id))
            } else {
                Err(error.into())
            }
        } else {
            Ok(())
        }
    }

    /// Writes the contents of the provided buffer at the offset from the given
    /// page id.
    ///
    /// # Errors
    ///
    /// - Fails if one of the I/O operations fails.
    ///
    /// # Panics
    ///
    /// - If `buf`'s length is different than [`PAGE_SIZE`].
    pub fn write_page(&mut self, page_id: PageId, buf: &Bytes) -> DbResult<()> {
        assert_eq!(buf.len() as u64, PAGE_SIZE);

        self.file.seek(SeekFrom::Start(page_id.offset()))?;
        self.file.write_all(buf)?;

        Ok(())
    }
}
