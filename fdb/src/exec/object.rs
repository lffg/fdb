use crate::{
    catalog::object::Object,
    error::{DbResult, Error},
    exec::query::{self, Query},
    Db,
};

impl Object {
    /// Tries to find the given object from the database.
    pub async fn find(db: &Db, name: &str) -> DbResult<Self> {
        let mut query = query::object::Select::new();
        while let Some(object) = query.next(db).await? {
            if object.name == name {
                return Ok(object);
            }
        }
        Err(Error::ExecError(format!("object `{name}` does not exist")))
    }
}
