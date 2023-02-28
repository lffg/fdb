use crate::{catalog::object::ObjectSchema, error::DbResult, pager::Pager};

pub mod common;
pub mod serde;

pub mod value;

mod insert;
pub use insert::*;

/// Execution context.
pub struct ExecCtx<'a> {
    pub pager: &'a mut Pager,
    pub object_schema: &'a ObjectSchema,
}

/// Executor trait. It is implemented for all database operations.
///
/// The database execution is based on the iterator model. This trait expresses
/// such an iterator. The `next` method may be called arbitrarily to lazily
/// fetch records without running out of memory.
///
/// The iterator `Item` is generic over the lifetime of [`ExecCtx`] since it may
/// borrow from such a context's fields, especially from the [`Pager`].
pub trait Executor {
    type Item<'a>;

    /// Produces the next value in the stream.
    fn next<'a>(&mut self, ctx: &'a mut ExecCtx) -> DbResult<Option<Self::Item<'a>>>;
}
