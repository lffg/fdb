use tracing::instrument;

use crate::{
    catalog::object::Object,
    error::{DbResult, Error},
    exec::query::QueryCtx,
};

/// Tries to find the [`Object`] with the given name. Fails otherwise.
#[instrument(skip(_ctx))]
pub fn find_object<'a>(_ctx: &QueryCtx<'a>, name: &str) -> DbResult<Object> {
    todo!();
}

/// Asserts that the given [`Object`] is a table.
pub fn object_is_not_table(object: &Object) -> Error {
    Error::ExecError(format!("object `{}` is not a table", object.name))
}
