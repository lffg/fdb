pub mod common;
pub mod serde;

pub mod value;

mod insert;
pub use insert::*;

use crate::{catalog::object::ObjectSchema, pager::Pager};

/// Execution context.
pub struct ExecCtx<'a> {
    pub pager: &'a mut Pager,
    pub object_schema: &'a ObjectSchema,
}
