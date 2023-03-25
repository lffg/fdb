use std::borrow::Cow;

use async_trait::async_trait;
use tracing::{debug, error, instrument};

use crate::{
    catalog::{
        object::Object,
        page::{HeapPage, PageId, SpecificPage},
        record::simple_record::{self, SimpleRecord},
    },
    error::{DbResult, Error},
    exec::{query::Query, util::macros::seq_h},
    io::pager::Pager,
    util::io::{Serialize, Size},
    Db,
};

const FIRST_SCHEMA_PAGE_ID: PageId = PageId::new_u32(2);

/// A create object query.
pub struct Create<'s> {
    object: &'s Object,
}

#[async_trait]
impl Query for Create<'_> {
    type Item<'a> = ();

    #[instrument(name = "ObjectCreate", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        let page_id = FIRST_SCHEMA_PAGE_ID;

        debug!(?page_id, "getting page");
        let guard = db.pager().get::<HeapPage>(page_id).await?;
        let mut page = guard.write().await;
        let last_page_id = seq_h!(mut page).last_page_id;

        let maybe_new_last_page_id = if last_page_id != page_id {
            // If there are more than one page in the heap sequence, one must
            // write into the last page in the sequence.
            debug!(?page_id, "getting last page");
            let last_guard = db.pager().get::<HeapPage>(last_page_id).await?;
            let mut last = last_guard.write().await;

            let mlp = write(db.pager(), &mut last, self.object).await?;
            last.flush();
            mlp
        } else {
            // Otherwise, one is in the first page.
            write(db.pager(), &mut page, self.object).await?
        };

        seq_h!(mut page).record_count += 1;
        if let Some(last_page_id) = maybe_new_last_page_id {
            page.header.next_page_id = Some(last_page_id);
            seq_h!(mut page).last_page_id = last_page_id;
            seq_h!(mut page).page_count += 1;
        }

        page.flush();

        db.pager().flush_all().await?;

        Ok(None)
    }
}

/// Writes the given `TableSchema` and, if allocated a new page, returns its ID.
#[instrument(level = "debug", skip_all)]
async fn write(pager: &Pager, page: &mut HeapPage, schema: &Object) -> DbResult<Option<PageId>> {
    let serde_ctx = simple_record::SimpleCtx {
        page_id: page.id(),
        offset: page.header.free_offset,
    };
    let record =
        SimpleRecord::<Object>::new(serde_ctx.page_id, serde_ctx.offset, Cow::Borrowed(schema));
    let size = record.size();

    if page.can_accommodate(size) {
        debug!("fit right in");
        page.write(|buf| record.serialize(buf))?;
        page.header.record_count += 1;

        return Ok(None);
    }

    // If the given page can't accommodate the given record, one must allocate a
    // new page.
    debug!("allocating new page to insert");
    let new_page_guard = pager.alloc(HeapPage::new_seq_node).await?;
    let mut new_page = new_page_guard.write().await;
    let new_page_id = new_page.id();

    // Sanity check.
    if !new_page.can_accommodate(size) {
        error!(size, "record size exceeded maximum page capacity");
        new_page.flush(); // TODO: Move this page to free list.

        return Err(Error::ExecError(format!(
            "record size ({size}) exceeds the maximum page capacity"
        )));
    }

    new_page.write(|buf| record.serialize(buf))?;
    new_page.header.record_count += 1;

    new_page.flush();

    Ok(Some(new_page_id))
}

impl<'s> Create<'s> {
    pub fn new(object: &'s Object) -> Create<'s> {
        Self { object }
    }
}
