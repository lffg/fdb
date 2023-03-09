use async_trait::async_trait;

use crate::{catalog::object::ObjectSchema, error::DbResult, io::pager::Pager};

mod insert;
pub use insert::*;

mod select;
pub use select::*;

/// Query execution context.
pub struct QueryCtx<'a> {
    pub pager: &'a Pager,
    pub object_schema: &'a ObjectSchema,
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
pub trait Executor {
    type Item<'a>;

    /// Produces the next value in the stream.
    async fn next<'a>(&mut self, ctx: &'a QueryCtx) -> DbResult<Option<Self::Item<'a>>>;
}