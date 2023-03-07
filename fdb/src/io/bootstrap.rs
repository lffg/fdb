use tracing::info;

use crate::{
    catalog::page::{FirstPage, PageId, SpecificPage},
    error::{DbResult, Error},
    io::pager::{Pager, PagerGuard},
};

/// Loads the first page, or bootstraps it in the case of first access.
///
/// It also returns a boolean that, if true, indicates that the page was booted
/// for the first time.
pub async fn boot_first_page(pager: &mut Pager) -> DbResult<(PagerGuard<FirstPage>, bool)> {
    match pager.get::<FirstPage>(PageId::FIRST).await {
        Ok(guard) => Ok((guard, false)),
        Err(Error::PageOutOfBounds(_)) => {
            info!("first access; booting first page");

            let first_page = FirstPage::default_with_id(PageId::FIRST);

            // SAFETY: This is the first page, no metadata is needed, yet.
            let guard = unsafe {
                pager.clear_cache(PageId::FIRST).await;
                pager.flush_page_and_build_guard(first_page).await?
            };

            Ok((guard, true))
        }
        Err(Error::ReadIncompletePage(_)) => {
            panic!("corrupt database file");
        }
        Err(error) => Err(error),
    }
}
