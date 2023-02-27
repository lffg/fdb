use buff::Buff;

use crate::{
    catalog::table_schema::TableSchema,
    error::{DbResult, Error},
    exec::value::{Environment, Value},
};

/// Serializes the record into the given `buf`.
///
/// The first byte is used to mark the record as "alive". It is always set to
/// zero.
pub fn serialize_table_record(
    buf: &mut Buff<'_>,
    schema: &TableSchema,
    env: &Environment,
) -> DbResult<()> {
    // Mark the record as alive.
    buf.write(false);

    // Serializes the record, on the order specified by the schema.
    for column in &schema.columns {
        let name = &column.name;
        let value = env
            .get(name)
            .unwrap_or_else(|| unimplemented!("default value"));

        if column.ty != value.type_id() {
            return Err(Error::ExecError(format!(
                "unexpected type for column `{name}`, expected of type `{}`, but got `{}`",
                column.ty.name(),
                value.type_id().name(),
            )));
        }

        value.serialize(buf)?;
    }

    Ok(())
}

/// Deserializes the record from the given `buf` into an [`Environment`] record.
///
/// TODO: Is [`Environment`] the best type to be returned by this function?
///
/// The first byte is used to determine the record's "aliveness". If the record
/// is marked as delete, `None` is returned.
pub fn _deserialize_table_record(
    buf: &mut Buff<'_>,
    schema: &TableSchema,
) -> DbResult<Option<Environment>> {
    // The first byte determines the record's aliveness. However, since this
    // database stores records in a sequential fashion, one still needs to
    // perform a "full record scan" to advance `buf`'s cursor to its end.
    //
    // Since this database soon won't be using sequential records anymore, I
    // won't bother optimizing for this kind of situation.
    let is_deleted: bool = buf.read();

    let mut env = Environment::default();
    for column in &schema.columns {
        let value = Value::deserialize(column.ty, buf)?;
        env.set(column.name.to_owned(), value);
    }

    Ok((!is_deleted).then_some(env))
}
