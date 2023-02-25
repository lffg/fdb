use std::{borrow::Cow, io};

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

    /// Corrupted header.
    #[error("corrupted header: {0}")]
    CorruptedHeader(&'static str),

    /// Invalid object type tag.
    #[error("corrupted object type tag")]
    CorruptedObjectTypeTag,

    /// Invalid type tag.
    #[error("corrupted type tag")]
    CorruptedTypeTag,

    /// Invalid heap page type tag.
    #[error("corrupted heap page type tag")]
    CorruptedHeapPageTypeTag,

    /// UTF-8 error.
    #[error("utf-8 error while decoding string for `{0}`")]
    CorruptedUtf8(Cow<'static, str>),

    /// Invalid size error.
    #[error("invalid size for `{name}`: expected at most {expected} bytes, but found {actual}")]
    SizeGreaterThanExpected {
        name: Cow<'static, str>,
        expected: usize,
        actual: usize,
    },

    /// An generic IO error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}
