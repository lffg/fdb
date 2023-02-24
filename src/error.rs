use std::io;

use crate::page::PageId;

pub type DbResult<T, E = Error> = Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The given page ID was out of bounds of the database file.
    #[error("page out of bounds ({0:?})")]
    PageOutOfBounds(PageId),

    /// Read an incomplete raw page, i.e., read less than PAGE_SIZE bytes.
    #[error("incomplete page ({0:?})")]
    ReadIncompletePage(PageId),

    /// An generic IO error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}
