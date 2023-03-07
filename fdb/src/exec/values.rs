use std::{borrow::Cow, collections::HashMap};

use crate::{
    catalog::table_schema::TableSchema,
    error::{DbResult, Error},
    exec::value::Value,
};

/// An environment that map from column names to database values ([`Value`]).
#[derive(Debug, Clone)]
pub struct Values {
    inner: HashMap<String, Value>,
}

impl Values {
    /// Constructs a new empty (and thus incomplete) values map.
    pub fn new() -> Values {
        Values {
            inner: HashMap::new(),
        }
    }

    /// Checks if the values already defined in the map met the given schema's
    /// column types requirements.
    ///
    /// It also completes the values map assigning default values for each
    /// unspecified value in the context of the provided schema.
    ///
    /// Returns the schematized values map if all column-typing constraint are
    /// met in the context of the provided [`TableSchema`].
    pub fn schematize<'a>(
        &'a mut self,
        schema: &'a TableSchema,
    ) -> DbResult<SchematizedValues<'a>> {
        SchematizedValues::try_new_borrowed(self, schema)
    }

    /// Returns a reference to the underlying value.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.inner.get(name)
    }

    /// Sets a value.
    pub fn set(&mut self, name: String, value: Value) {
        self.inner.insert(name, value);
    }
}

impl Default for Values {
    fn default() -> Self {
        Self::new()
    }
}

impl From<HashMap<String, Value>> for Values {
    fn from(inner: HashMap<String, Value>) -> Values {
        Values { inner }
    }
}

/// An schematized environment. See [`Values`].
pub struct SchematizedValues<'a> {
    values: Cow<'a, Values>,
    schema: &'a TableSchema,
    /// The `values` size.
    ///
    /// Since [`SchematizedValues`] wraps over a shared (i.e., immutable)
    /// reference to the table schema, one can be sure that the [`Values`] size
    /// won't change for the lifetime of this [`SchematizedValues`]'s instance.
    /// Hence, the `size` field may be computed upon the instance's creation.
    size: u32,
}

/// Wraps over a [`Values`] map that is complete and valid in the context of
/// some schema.
///
/// Only schematized [`Values`] maps may be serialized.

impl SchematizedValues<'_> {
    /// Returns the total size of the values map.
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Serializes the map's values in the order specified by the schema columns
    /// list.
    pub fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        for column in &self.schema.columns {
            let value = self.values.get(&column.name).expect("is schematized");
            value.serialize(buf)?;
        }
        Ok(())
    }

    /// Deserializes the map's values in the order specified by the schema
    /// columns list.
    pub fn deserialize<'a>(
        buf: &mut buff::Buff<'_>,
        schema: &'a TableSchema,
    ) -> DbResult<SchematizedValues<'a>>
    where
        Self: Sized,
    {
        let mut inner = HashMap::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let value = Value::deserialize(column.ty, buf)?;
            inner.insert(column.name.to_owned(), value);
        }
        // SAFETY: Database assumes that is just stores valid records.
        Ok(unsafe { Self::try_new_unchecked(Cow::Owned(Values::from(inner)), schema) })
    }

    /// Returns the underlying owned [`Values`].
    ///
    /// This method *may* clone the underlying [`Values`] map.
    pub fn into_values(self) -> Values {
        self.values.into_owned()
    }

    /// Tries to construct a new schematized values over a "raw" values map and
    /// a schema.
    fn try_new_borrowed<'a>(
        values: &'a mut Values,
        schema: &'a TableSchema,
    ) -> DbResult<SchematizedValues<'a>> {
        let mut size = 0;

        for column in &schema.columns {
            let name = &column.name;
            match values.inner.get(name) {
                Some(value) => {
                    size += value.size();
                    if column.ty != value.type_id() {
                        return Err(Error::ExecError(format!(
                            "unexpected type for column `{name}`, expected of type `{}`, but got `{}`",
                            column.ty.name(),
                            value.type_id().name(),
                        )));
                    }
                }
                None => {
                    // TODO: Required fields in schema.
                    let value = Value::default_for_type(column.ty);
                    size += value.size();
                    values.inner.insert(column.name.clone(), value);
                }
            }
        }

        Ok(SchematizedValues {
            values: Cow::Borrowed(values),
            schema,
            size,
        })
    }

    /// Constructs a new [`SchematizedValues`] without checking for types and
    /// nullability of the values.
    ///
    /// # Safety
    unsafe fn try_new_unchecked<'a>(
        values: Cow<'a, Values>,
        schema: &'a TableSchema,
    ) -> SchematizedValues<'a> {
        let size = values.inner.values().map(Value::size).sum();
        SchematizedValues {
            values,
            schema,
            size,
        }
    }
}
