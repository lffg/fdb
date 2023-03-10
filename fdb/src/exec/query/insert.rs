use std::borrow::Cow;

use async_trait::async_trait;
use tracing::{error, instrument, trace};

use crate::{
    catalog::{
        object::Object,
        page::{HeapPage, PageId, SpecificPage},
        record::simple_record::{self, SimpleRecord},
        table_schema::TableSchema,
    },
    error::{DbResult, Error},
    exec::{
        query::{seq_h, Executor, QueryCtx},
        values::{SchematizedValues, Values},
    },
    io::pager::Pager,
    util::io::{SerdeCtx, Size},
};

/// An insert operation.
pub struct Insert<'s> {
    /// The table name.
    table_name: &'s str,
    /// The values to be inserted.
    values: Values,
}

#[async_trait]
impl Executor for Insert<'_> {
    type Item<'a> = ();

    #[instrument(skip_all)]
    async fn next<'a>(&mut self, ctx: &'a QueryCtx<'a>) -> DbResult<Option<Self::Item<'a>>> {
        let object = Object::find(ctx, self.table_name).await?;
        let page_id = object.page_id;

        let table_schema = object.try_into_table_schema()?;
        let schematized_values = self.values.schematize(&table_schema)?;

        trace!(?page_id, "getting page");
        let guard = ctx.pager.get::<HeapPage>(page_id).await?;
        let mut page = guard.write().await;
        let last_page_id = seq_h!(page).last_page_id;

        let maybe_new_last_page_id = if last_page_id != page_id {
            // If there are more than one page in the heap sequence, one must
            // write into the last page in the sequence.
            trace!(?page_id, "getting last page");
            let last_guard = ctx.pager.get::<HeapPage>(last_page_id).await?;
            let mut last = last_guard.write().await;

            let mlp = write(ctx.pager, &mut last, &table_schema, &schematized_values).await?;
            last.flush();
            mlp
        } else {
            // Otherwise, one is in the first page.
            write(ctx.pager, &mut page, &table_schema, &schematized_values).await?
        };

        let seq_h = seq_h!(page);
        seq_h.record_count += 1;
        if let Some(last_page_id) = maybe_new_last_page_id {
            seq_h.last_page_id = last_page_id;
            seq_h.page_count += 1;
        }

        page.flush();

        ctx.pager.flush_all().await?;

        Ok(None)
    }
}

/// Writes the given `TableSchema` and, if allocated a new page, returns its ID.
#[instrument(skip_all)]
async fn write(
    pager: &Pager,
    page: &mut HeapPage,
    schema: &TableSchema,
    record: &SchematizedValues<'_>,
) -> DbResult<Option<PageId>> {
    let serde_ctx = simple_record::TableRecordCtx {
        schema,
        offset: page.offset(),
    };
    let record = SimpleRecord::<SchematizedValues>::new(serde_ctx.offset, Cow::Borrowed(record));
    let size = record.size();

    if page.can_accommodate(size) {
        trace!("fit right in");
        page.write(|buf| record.serialize(buf, serde_ctx))?;
        page.header.record_count += 1;

        return Ok(None);
    }

    // If the given page can't accommodate the given record, one must allocate a
    // new page.
    trace!("allocating new page to insert");
    let new_page_guard = pager.alloc::<HeapPage>().await?;
    let new_page = new_page_guard.write().await;
    let new_page_id = new_page.id();

    // Sanity check.
    if !new_page.can_accommodate(size) {
        error!(size, "record size exceeded maximum page capacity");
        new_page.flush(); // TODO: Move this page to free list.

        return Err(Error::ExecError(format!(
            "record size ({size}) exceeds the maximum page capacity"
        )));
    }

    page.write(|buf| record.serialize(buf, serde_ctx))?;
    page.header.record_count += 1;

    new_page.flush();

    Ok(Some(new_page_id))
}

impl<'s> Insert<'s> {
    /// Creates a new insert executor.
    pub fn new(table_name: &'s str, values: Values) -> Insert<'s> {
        Self { table_name, values }
    }
}
