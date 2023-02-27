use crate::{
    catalog::{page::PageId, table_schema::TableSchema},
    error::{DbResult, Error},
    ioutil::{BuffExt, Serde},
};

/// The database object catalog; i.e., the collection of all the objects
/// contained in the database. For more information on objects, see [`Object`].
///
/// The database object schema is linked multi-page structure which defines all
/// database objects.
#[derive(Debug)]
pub struct ObjectSchema {
    pub next_id: Option<PageId>,
    pub object_count: u16,
    pub objects: Vec<Object>,
}

impl Serde for ObjectSchema {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write_page_id(self.next_id);
        buf.write(self.object_count);
        debug_assert_eq!(self.object_count as usize, self.objects.len());
        for object in &self.objects {
            object.serialize(buf)?;
        }
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let next_id = buf.read_page_id();
        let object_count: u16 = buf.read();
        let objects: Vec<_> = (0..object_count)
            .map(|_| Object::deserialize(buf))
            .collect::<Result<_, _>>()?;
        Ok(ObjectSchema {
            next_id,
            object_count,
            objects,
        })
    }
}

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

impl Serde for Object {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.ty.serialize(buf)?;
        buf.write_page_id(Some(self.page_id));
        buf.write_var_size_string(&self.name)?;
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let ty = ObjectType::deserialize(buf)?;
        let page_id = buf.read_page_id().expect("non-null page id");
        let name = buf.read_var_size_string()?;
        Ok(Object { ty, page_id, name })
    }
}

/// An [`Object`] type.
#[derive(Debug, Clone)]
pub enum ObjectType {
    Table(TableSchema),
    Index,
}

impl Serde for ObjectType {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.discriminant());
        if let ObjectType::Table(table_schema) = self {
            table_schema.serialize(buf)?;
        }
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let tag: u8 = buf.read();
        match tag {
            0xA => {
                let table_schema = TableSchema::deserialize(buf)?;
                Ok(ObjectType::Table(table_schema))
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
