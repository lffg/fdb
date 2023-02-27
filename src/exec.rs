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

pub trait Command {
    type Ret;

    fn execute(self, ctx: &mut ExecCtx) -> DbResult<Self::Ret>;
}
