use std::{borrow::Cow, collections::HashMap};

use crate::{
    catalog::table_schema::TableSchema,
    error::{DbResult, Error},
    exec::value::Value,
    util::io::{SerdeCtx, Size},
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
/// some schema.
///
/// Only schematized [`Values`] maps may be serialized and deserialized.
///
/// This type can only be constructed after validating the [`Values`] over a
/// schema.
#[derive(Debug, Clone)]
pub struct SchematizedValues<'a> {
    values: Cow<'a, Values>,
    size: u32,
}

impl Size for SchematizedValues<'_> {
    fn size(&self) -> u32 {
        self.size
    }
}

impl SerdeCtx<'_, &TableSchema, &TableSchema> for SchematizedValues<'_> {
    fn serialize(&self, buf: &mut buff::Buff<'_>, schema: &TableSchema) -> DbResult<()> {
        for column in &schema.columns {
            let value = self.values.get(&column.name).expect("is schematized");
            value.serialize(buf, ())?;
        }
        Ok(())
    }

    fn deserialize(
        buf: &mut buff::Buff<'_>,
        schema: &TableSchema,
    ) -> DbResult<SchematizedValues<'static>>
    where
        Self: Sized,
    {
        let mut inner = HashMap::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let value = Value::deserialize(buf, column.ty)?;
            inner.insert(column.name.to_owned(), value);
        }
        // SAFETY: Database assumes that is just stores valid records.
        Ok(unsafe { Self::try_new_unchecked(Cow::Owned(Values::from(inner))) })
    }
}

impl SchematizedValues<'_> {
    /// Returns the underlying owned [`Values`].
    ///
    /// This method *may* clone the underlying [`Values`] map.
    pub fn into_values(self) -> Values {
        self.values.into_owned()
    }

    /// Tries to construct a new schematized values over a "raw" values map and
    /// a schema.
    ///
    /// `values` is modified in place if it has any null value.
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
            size,
        })
    }

    /// Constructs a new [`SchematizedValues`] without checking for types and
    /// nullability of the values.
    ///
    /// # Safety
    unsafe fn try_new_unchecked(values: Cow<'_, Values>) -> SchematizedValues<'_> {
        let size = values.inner.values().map(Value::size).sum();
        SchematizedValues { values, size }
    }
}
