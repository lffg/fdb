use crate::{
    catalog::{page::PageId, table_schema::TableSchema},
    error::{DbResult, Error},
    util::io::{Serde, VarList, VarString},
};

/// The database object catalog; i.e., the collection of all the objects
/// contained in the database. For more information on objects, see [`Object`].
///
/// The database object schema is linked multi-page structure which defines all
/// database objects.
#[derive(Debug)]
pub struct ObjectSchema {
    pub next_id: Option<PageId>,
    pub objects: Vec<Object>,
}

impl Serde<'_> for ObjectSchema {
    fn size(&self) -> u32 {
        self.next_id.size() + VarList::from(self.objects.as_slice()).size()
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.next_id.serialize(buf)?;
        VarList::from(self.objects.as_slice()).serialize(buf)?;
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(ObjectSchema {
            next_id: Option::<PageId>::deserialize(buf)?,
            objects: VarList::deserialize(buf)?.into(),
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

impl Serde<'_> for Object {
    fn size(&self) -> u32 {
        self.ty.size() + self.page_id.size() + VarString::from(self.name.as_str()).size()
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.ty.serialize(buf)?;
        self.page_id.serialize(buf)?;
        VarString::from(self.name.as_str()).serialize(buf)?;
        Ok(())
    }

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

impl Serde<'_> for ObjectType {
    fn size(&self) -> u32 {
        1 + match self {
            ObjectType::Table(schema) => schema.size(),
            ObjectType::Index => 0,
        }
    }

    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.discriminant());
        if let ObjectType::Table(schema) = self {
            schema.serialize(buf)?;
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
