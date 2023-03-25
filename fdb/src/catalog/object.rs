use crate::{
    catalog::{page::PageId, table_schema::TableSchema},
    error::{DbResult, Error},
    util::io::{Deserialize, Serialize, Size, VarString},
};

/// The database object definition. From the database's point of view, an
/// "object" is a structured group of information; for example, a table, an
/// index, etc.
#[derive(Debug, Clone)]
pub struct Object {
    /// The object's type (e.g. a table, an index, etc).
    pub ty: ObjectType,
    /// The ID of the first page that stores the actual records.
    pub page_id: PageId,
    /// The object name (e.g. the table name as per the user's definition).
    ///
    /// The object name (i.e., a table name or an index name) may have at most
    /// 64 bytes.
    pub name: String,
}

impl Size for Object {
    fn size(&self) -> u32 {
        self.ty.size() + self.page_id.size() + VarString::from(self.name.as_str()).size()
    }
}

impl Serialize for Object {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.ty.serialize(buf)?;
        self.page_id.serialize(buf)?;
        VarString::from(self.name.as_str()).serialize(buf)?;
        Ok(())
    }
}

impl Deserialize<'_> for Object {
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let ty = ObjectType::deserialize(buf)?;
        let page_id = PageId::deserialize(buf)?;
        let name = VarString::deserialize(buf)?.into();
        Ok(Object { ty, page_id, name })
    }
}

/// An [`Object`] type.
#[derive(Debug, Clone)]
pub enum ObjectType {
    Table(TableSchema),
    Index,
}

impl Size for ObjectType {
    fn size(&self) -> u32 {
        1 + match self {
            ObjectType::Table(schema) => schema.size(),
            ObjectType::Index => 0,
        }
    }
}

impl Serialize for ObjectType {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.discriminant());
        if let ObjectType::Table(schema) = self {
            schema.serialize(buf)?;
        }
        Ok(())
    }
}

impl Deserialize<'_> for ObjectType {
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let tag: u8 = buf.read();
        match tag {
            0xA => {
                let schema = TableSchema::deserialize(buf)?;
                Ok(ObjectType::Table(schema))
            }
            0xB => Ok(ObjectType::Index),
            _ => Err(Error::CorruptedObjectTypeTag),
        }
    }
}

impl ObjectType {
    /// Returns the tag associated with the `HeapPageId`.
    pub const fn discriminant(&self) -> u8 {
        match self {
            ObjectType::Table(_) => 0xA,
            ObjectType::Index => 0xB,
        }
    }

    /// Returns the name of the object type.
    pub const fn _name(&self) -> &'static str {
        match self {
            ObjectType::Table(_) => "table",
            ObjectType::Index => "index",
        }
    }
}

/// A table object type.
#[derive(Debug)]
pub struct TableObject {
    pub schema: TableSchema,
    pub page_id: PageId,
    pub name: String,
}

impl Object {
    /// Returns the underlying [`TableObject`] or fails.
    pub fn try_into_table(self) -> DbResult<TableObject> {
        if let ObjectType::Table(schema) = self.ty {
            Ok(TableObject {
                schema,
                page_id: self.page_id,
                name: self.name,
            })
        } else {
            Err(Error::Cast(format!(
                "object `{}` is not a table",
                self.name
            )))
        }
    }
}
