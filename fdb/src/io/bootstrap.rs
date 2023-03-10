use tracing::{debug, instrument};

use crate::{
    catalog::page::{FirstPage, HeapPage, PageId},
    error::{DbResult, Error},
    io::pager::Pager,
};

/// Loads the first page, or bootstraps it in the case of first access.
///
/// It also returns a boolean that, if true, indicates that the page was booted
/// for the first time.
#[instrument(level = "debug", skip_all)]
pub async fn boot_first_page(pager: &mut Pager) -> DbResult<bool> {
    match pager.get::<FirstPage>(PageId::FIRST).await {
        Ok(_) => Ok(false),
        Err(Error::PageOutOfBounds(_)) => {
            debug!("first access; booting first page");

            let first_page = FirstPage::new();

            // SAFETY: This is the first page, no metadata is needed, yet.
            unsafe {
                pager.clear_cache(PageId::FIRST).await;
                pager.flush_page_and_build_guard(first_page).await?;
            }

            // Allocates an empty heap page to accommodate the database schema.
            pager.alloc(HeapPage::new_seq_first).await?;

            Ok(true)
        }
        Err(Error::ReadIncompletePage(_)) => {
            panic!("corrupt database file");
        }
        Err(error) => Err(error),
    }
}
