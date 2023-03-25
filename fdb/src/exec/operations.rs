use crate::catalog::page::PageId;

pub mod heap {
    mod seq_scan;
    pub use seq_scan::*;
}

#[derive(Copy, Clone, Debug)]
pub struct PhysicalState {
    pub page_id: PageId,
    pub offset: u16,
}
