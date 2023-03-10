use crate::{
    catalog::{
        object::{Object, ObjectType},
        table_schema::TableSchema,
    },
    error::{DbResult, Error},
    exec::query::{Executor, ObjectSelect, QueryCtx},
};

impl Object {
    /// Tries to find the given object from the database.
    pub async fn find(ctx: &QueryCtx<'_>, name: &str) -> DbResult<Self> {
        let mut query = ObjectSelect::new();
        while let Some(object) = query.next(ctx).await? {
            if object.name == name {
                return Ok(object);
            }
        }
        Err(Error::ExecError(format!("table `{name}` does not exist")))
    }

    /// Returns the underlying [`TableSchema`] or fails.
    pub fn try_into_table_schema(self) -> DbResult<TableSchema> {
        if let ObjectType::Table(table) = self.ty {
            Ok(table)
        } else {
            Err(Error::ExecError(format!(
                "object `{}` is not a table",
                self.name
            )))
        }
    }
}
