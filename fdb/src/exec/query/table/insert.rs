use std::borrow::Cow;

use async_trait::async_trait;
use tracing::{debug, error, instrument};

use crate::{
    catalog::{
        object::TableObject,
        page::{HeapPage, PageId, SpecificPage},
        record::simple_record::{self, SimpleRecord},
        table_schema::TableSchema,
    },
    error::{DbResult, Error},
    exec::{
        query::{seq_h, Query},
        values::{SchematizedValues, Values},
    },
    io::pager::Pager,
    util::io::{SerdeCtx, Size},
    Db,
};

/// An insert query.
pub struct Insert<'a> {
    /// The table object.
    table: &'a TableObject,
    /// The values to be inserted.
    values: Values,
}

#[async_trait]
impl Query for Insert<'_> {
    type Item<'a> = ();

    #[instrument(name = "TableInsert", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        let page_id = self.table.page_id;
        let table_schema = &self.table.schema;
        let schematized_values = self.values.schematize(table_schema)?;

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

            let mlp = write(db.pager(), &mut last, table_schema, &schematized_values).await?;
            last.flush();
            mlp
        } else {
            // Otherwise, one is in the first page.
            write(db.pager(), &mut page, table_schema, &schematized_values).await?
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
async fn write(
    pager: &Pager,
    page: &mut HeapPage,
    schema: &TableSchema,
    record: &SchematizedValues<'_>,
) -> DbResult<Option<PageId>> {
    let serde_ctx = simple_record::TableRecordCtx {
        page_id: page.id(),
        offset: page.offset(),
        schema,
    };
    let record = SimpleRecord::<SchematizedValues>::new(
        serde_ctx.page_id,
        serde_ctx.offset,
        Cow::Borrowed(record),
    );
    let size = record.size();

    if page.can_accommodate(size) {
        debug!("fit right in");
        page.write(|buf| record.serialize(buf, serde_ctx))?;
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

    new_page.write(|buf| record.serialize(buf, serde_ctx))?;
    new_page.header.record_count += 1;

    new_page.flush();

    Ok(Some(new_page_id))
}

impl<'a> Insert<'a> {
    /// Creates a new insert executor.
    pub fn new(table: &'a TableObject, values: Values) -> Insert<'a> {
        Self { table, values }
    }
}
