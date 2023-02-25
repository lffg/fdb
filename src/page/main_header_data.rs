use crate::page::PageId;

/// The database header.
#[derive(Debug)]
pub struct MainHeaderData {
    /// The file format version. Currently, such a version is defined as `0`.
    pub file_format_version: u8,
    /// The total number of pages being used in the file.
    pub page_count: u32,
    /// The ID of the first free list page.
    pub first_free_list_page_id: Option<PageId>,
}
