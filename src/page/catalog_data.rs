use crate::{catalog, page::PageId};

/// A catalog page wraps definitions of database objects.
///
/// The first catalog page is stored within the [`FirstPage`]. If the database
/// catalog can't fit in there, other catalog pages may be stored in heap pages;
/// hence, the `next_id` field.
#[derive(Debug)]
pub struct CatalogData {
    // TODO(P0): See if this representation fits in the slotted page approach.
    // It MUST since HEAP PAGES will work using an slotted page approach.
    pub next_id: Option<PageId>,
    pub object_count: u16,
    pub objects: Vec<catalog::Object>,
}
