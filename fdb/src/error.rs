use std::{io, sync::Arc};

use crate::catalog::page::PageId;

pub type DbResult<T, E = Error> = Result<T, E>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The given page ID was out of bounds of the database file.
    #[error("page out of bounds ({0:?})")]
    PageOutOfBounds(PageId),

    /// Read an incomplete raw page, i.e., read less than PAGE_SIZE bytes.
    #[error("incomplete page ({0:?})")]
    ReadIncompletePage(PageId),

    /// Corrupted header.
    #[error("corrupted header: {0}")]
    CorruptedHeader(&'static str),

    /// Invalid object type tag.
    #[error("corrupted object type tag")]
    CorruptedObjectTypeTag,

    /// Invalid type tag.
    #[error("corrupted type tag")]
    CorruptedTypeTag,

    /// UTF-8 error.
    #[error("utf-8 error while decoding string")]
    CorruptedUtf8,

    /// Casting error.
    #[error("cast error: {0}")]
    Cast(String),

    /// Generic error.
    #[error("execution error: {0}")]
    ExecError(String),

    /// An generic IO error.
    #[error("io error: {0}")]
    Io(Arc<io::Error>),
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(Arc::new(value))
    }
}
