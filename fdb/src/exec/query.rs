use async_trait::async_trait;

use crate::{error::DbResult, io::pager::Pager};

pub mod object {
    mod create;
    pub use create::*;

    mod select;
    pub use select::*;
}

pub mod table {
    mod insert;
    pub use insert::*;

    mod select;
    pub use select::*;
}

/// Query execution context.
pub struct QueryCtx<'a> {
    pub pager: &'a Pager,
}

/// Query execution trait. It is implemented for all database operations.
///
/// The database execution is based on the iterator model. This trait expresses
/// such an iterator. The `next` method may be called arbitrarily to lazily
/// fetch records without running out of memory.
///
/// The element type `Item` is generic over the lifetime of [`QueryCtx`] since
/// it may borrow from such a context's fields, especially from the [`Pager`].
#[async_trait]
pub trait Query {
    type Item<'a>;

    /// Produces the next value in the stream.
    async fn next<'a>(&mut self, ctx: &'a QueryCtx) -> DbResult<Option<Self::Item<'a>>>;
}

macro_rules! seq_h {
    (mut $guard:expr) => {
        $guard.header.seq_header.as_mut().expect("first page")
    };
    ($guard:expr) => {
        $guard.header.seq_header.as_ref().expect("first page")
    };
}
use seq_h;
