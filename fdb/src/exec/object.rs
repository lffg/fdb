use tracing::{info, instrument};

use crate::{
    catalog::object::Object,
    error::{DbResult, Error},
    exec::query::QueryCtx,
};

/// Tries to find the [`Object`] with the given name. Fails otherwise.
#[instrument(skip(ctx))]
pub fn find_object<'a>(ctx: &QueryCtx<'a>, name: &str) -> DbResult<Object> {
    let next = ctx.object_schema.next_id;

    let object = ctx
        .object_schema
        .objects
        .iter()
        .find(|object| object.name == name)
        .cloned();

    while object.is_none() && next.is_some() {
        info!("next page");
        todo!("TODO: implement next object schema page");
    }

    object.ok_or_else(|| Error::ExecError(format!("table object `{name}` does not exist")))
}

/// Asserts that the given [`Object`] is a table.
pub fn object_is_not_table(object: &Object) -> Error {
    Error::ExecError(format!("object `{}` is not a table", object.name))
}
