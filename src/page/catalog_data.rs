use crate::{
    catalog,
    error::DbResult,
    ioutil::{BuffExt, Serde},
    page::PageId,
};

/// A catalog page wraps definitions of database objects.
///
/// The first catalog page is stored in the `FirstPage`. If the database catalog
/// can't fit in there, other catalog pages may be stored in heap pages; hence,
/// the `next_id` field.
#[derive(Debug)]
pub struct CatalogData {
    // TODO(P0): See if this representation fits in the slotted page approach.
    // It MUST since HEAP PAGES will work using an slotted page approach.
    pub next_id: Option<PageId>,
    pub object_count: u16,
    pub objects: Vec<catalog::Object>,
}

impl Serde for CatalogData {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write_page_id(self.next_id);
        buf.write(self.object_count);
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
            .map(|_| catalog::Object::deserialize(buf))
            .collect::<Result<_, _>>()?;
        Ok(CatalogData {
            next_id,
            object_count,
            objects,
        })
    }
}
