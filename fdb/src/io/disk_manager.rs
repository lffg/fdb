use std::{
    io::{self, SeekFrom},
    path::Path,
};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    catalog::page::PageId,
    config::PAGE_SIZE,
    error::{DbResult, Error},
};

pub struct DiskManager {
    file: File,
}

impl DiskManager {
    /// Opens the file at the provided path and constructs a new disk manager
    /// instance that wraps over it.
    pub async fn new(path: &Path) -> DbResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // TODO: Add `O_DIRECT` flag.
            .open(path)
            .await?;

        Ok(DiskManager { file })
    }

    /// Reads the contents of the page at the offset from the given page id,
    /// writing them at the provided buffer.
    ///
    /// # Panics
    ///
    /// - If `buf`'s length is different than [`PAGE_SIZE`].
    pub async fn read_page(&mut self, page_id: PageId, buf: &mut [u8]) -> DbResult<()> {
        assert_eq!(buf.len() as u64, PAGE_SIZE);

        let size = self.file.metadata().await?.len();
        let offset = page_id.offset();
        if offset >= size {
            return Err(Error::PageOutOfBounds(page_id));
        }

        self.file.seek(SeekFrom::Start(page_id.offset())).await?;

        if let Err(error) = self.file.read_exact(buf).await {
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
    /// # Panics
    ///
    /// - If `buf`'s length is different than [`PAGE_SIZE`].
    pub async fn write_page(&mut self, page_id: PageId, buf: &[u8]) -> DbResult<()> {
        assert_eq!(buf.len() as u64, PAGE_SIZE);

        self.file.seek(SeekFrom::Start(page_id.offset())).await?;
        self.file.write_all(buf).await?;

        Ok(())
    }
}
