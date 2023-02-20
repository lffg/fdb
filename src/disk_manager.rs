use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use bytes::{Bytes, BytesMut};

use crate::{config::PAGE_SIZE, page::PageId};

pub struct DiskManager {
    file: File,
}

impl DiskManager {
    /// Opens the file at the provided path and constructs a new disk manager
    /// instance that wraps over it.
    pub fn new(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
    pub fn read_page(&mut self, page_id: PageId, buf: &mut BytesMut) -> io::Result<()> {
        assert_eq!(buf.len() as u64, PAGE_SIZE);

        let size = self.file.metadata()?.len();
        let offset = page_id.offset();
        if offset >= size {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        self.file.seek(SeekFrom::Start(page_id.offset()))?;

        if let Err(error) = self.file.read_exact(&mut buf[..]) {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                unreachable!("read page with less than PAGE_SIZE bytes");
            } else {
                return Err(error);
            }
        }

        Ok(())
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
    pub fn write_page(&mut self, page_id: PageId, buf: &Bytes) -> io::Result<()> {
        assert_eq!(buf.len() as u64, PAGE_SIZE);

        self.file.seek(SeekFrom::Start(page_id.offset()))?;
        self.file.write_all(buf)?;

        Ok(())
    }
}
