use std::{
    path::{Path, PathBuf},
    sync::atomic::{self, AtomicU32},
};

use async_trait::async_trait;
use tokio::fs::{File, OpenOptions};
use tracing::{instrument, trace};

use crate::{
    catalog::{object::TableObject, page::PageId, record::simple_record::SimpleRecord},
    error::{DbResult, Error},
    exec::{
        query::{table::linear_scan::LinearScan, Query},
        util::{
            cmp::{new_boxed_cmp_fn, OrderBy},
            macros::get_or_insert_with,
            size_buf::SizeBuf,
        },
        values::SchematizedValues,
    },
    Db,
};

// NOTES
//
// 1. I am implementing bulk insert so that this module may use it.
// 2. I also need to implement a `CREATE TABLE` functionality. Also `DROP TABLE`
//    and a free page list. T-T

// XXX: Extract this `RecordData` as a type parameter and make the sort
// implementation generic over sortable types.
type RecordData = SchematizedValues<'static>;

type Record = SimpleRecord<'static, RecordData>;

/// Sort options.
pub struct SortOpts<'a> {
    /// The maximum number of tapes that the algorithm may use.
    ///
    /// XXX: Optimize the number of temporary files.
    ///
    /// The implementation will allocate `tapes * 2` temporary files.
    pub tapes: u16,
    /// The maximum number of page sizes that should be allocated while loading
    /// pages from the disk. I.e., the maximum working memory that shall be
    /// allocated for the sort operation.
    ///
    /// FIXME: Determine minimum value.
    pub work_mem_pages: usize,
    /// The page size.
    pub page_size: u16,
    /// The page size.
    /// The name of the columns to be ordered by.
    pub order_by: OrderBy<'a>,
}

impl SortOpts<'_> {
    /// Returns the total available memory.
    fn work_mem(&self) -> usize {
        self.work_mem_pages * self.page_size as usize
    }
}

/// A sort query.
pub struct Sort<'a> {
    table: &'a TableObject,
    opts: SortOpts<'a>,
    id: u32,
    sort_outcome_iter: Option<SortOutcomeIter>,
}

#[async_trait]
impl Query for Sort<'_> {
    /// [`Sort`] doesn't yield [`Record`]s since tuples come from temporary
    /// files. Hence, they don't have all properties that a record must have,
    /// such as an offset, etc.
    type Item<'a> = RecordData;

    #[instrument(name = "TableSort", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        let iter = get_or_insert_with!(&mut self.sort_outcome_iter, || {
            SortOutcomeIter::from(self.perform_sort(db).await?)
        });
        match iter {
            SortOutcomeIter::Internal(iter) => Ok(iter.next()),
            SortOutcomeIter::External(_, _, _) => unimplemented!(""),
        }
    }
}

impl Sort<'_> {
    /// Sorts the records in the table.
    ///
    /// If all the records fit in `work_mem_pages`, a simple in-memory sorting
    /// takes place. Otherwise, external sorting takes place.
    ///
    /// In the case of external sorting, this method distributes all records
    /// in separate sorted runs and then performs a K-way merge operation to
    /// unify them in a sorted manner.
    async fn perform_sort(&mut self, db: &Db) -> DbResult<SortOutcome> {
        let cmp_fn = new_boxed_cmp_fn(self.table, self.opts.order_by)?;

        let mut records_src = LinearScan::new(self.table);
        let mut buf = SizeBuf::<RecordData>::new(self.opts.work_mem());

        let mut distr_tapes_ctl = TapesCtl::new(self.id, "a", self.opts.tapes);

        // Distribution phase.
        for i in 0.. {
            // Fills the buffer.
            let buf_status = self.fill_buf(db, &mut records_src, &mut buf).await?;

            buf.as_inner_mut()
                .sort_unstable_by(|a, b| cmp_fn(a.as_values(), b.as_values()));

            // If one is in the first iteration and the records source was
            // exhausted, sorting may take place in the primary memory.
            if buf_status == LoadBufStatus::Finished && i == 0 {
                return Ok(SortOutcome::Internal(buf.into_inner()));
            }

            // ... Otherwise, one assumes external sorting. Now the sorted
            // buffer should be emitted as a run.
            let (run, tape) = distr_tapes_ctl.next_run_with_tape().await?;
        }
        todo!();
    }

    /// Fills the given buffer until it has no more capacity or the underlying
    /// tuple source is exhausted.
    async fn fill_buf(
        &mut self,
        db: &Db,
        src: &mut LinearScan<'_>,
        buf: &mut SizeBuf<RecordData>,
    ) -> DbResult<LoadBufStatus> {
        buf.clear();
        loop {
            let Some(record) = src.peek(db).await? else {
                return Ok(LoadBufStatus::Finished);
            };
            if let Err(_) = buf.try_push(record.into_data().into_owned()) {
                return Ok(LoadBufStatus::Filled);
            }
            // Advances the underlying cursor.
            //
            // XXX: Should `LinearScan` provide an unsafe `advance_by` method
            // to avoid a re-deserialization?
            let next_result = src.next(db).await?;
            debug_assert!(next_result.is_some());
        }
    }
}

impl<'a> Sort<'a> {
    pub fn new(table: &'a TableObject, opts: SortOpts<'a>) -> Sort<'a> {
        static SORT_ID: AtomicU32 = AtomicU32::new(0);
        let id = SORT_ID.fetch_add(1, atomic::Ordering::AcqRel);

        Self {
            table,
            opts,
            id,
            sort_outcome_iter: None,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum LoadBufStatus {
    Filled,
    Finished,
}

/// A list of tapes and runs.
struct TapesCtl {
    sort_id: u32,
    prefix: &'static str,
    max_tape_count: u16,
    tapes: Vec<Tape>,
    runs: Vec<Run>,
}

impl TapesCtl {
    fn new(sort_id: u32, prefix: &'static str, max_tape_count: u16) -> Self {
        TapesCtl {
            sort_id,
            prefix,
            max_tape_count,
            tapes: Vec::new(),
            runs: Vec::new(),
        }
    }

    /// Returns the next run with tape, for reading or writing.
    async fn next_run_with_tape(&mut self) -> DbResult<(&mut Run, &mut Tape)> {
        let run_i = self.runs.len();
        self.runs.push(Default::default());

        let tape_i = run_i % self.max_tape_count as usize;
        if tape_i >= self.tapes.len() {
            trace!(tape_i, "allocating new tape");
            self.tapes.push(self.alloc_tape(tape_i).await?);
        }

        // SAFETY: Both indexes are valid, as checked above.
        Ok(unsafe {
            (
                self.runs.get_unchecked_mut(run_i),
                self.tapes.get_unchecked_mut(tape_i),
            )
        })
    }

    /// Allocates a new tape.
    async fn alloc_tape(&self, i: usize) -> DbResult<Tape> {
        let path = PathBuf::from(format!(
            "tmp-sort-{sort_id}-{prefix}-{i}",
            sort_id = self.sort_id,
            prefix = self.prefix
        ));
        Tape::alloc(&path).await
    }
}

/// A tape.
struct Tape {
    file: File,
    last_page_id: PageId,
}

impl Tape {
    async fn alloc(path: &Path) -> DbResult<Self> {
        Ok(Tape {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)
                .await?,
            last_page_id: PageId::FIRST,
        })
    }
}

/// A run.
#[derive(Default)]
struct Run {
    /// The number of elements in this run.
    total: u64,
}

enum SortOutcome {
    Internal(Vec<RecordData>),
    External(Run, Tape),
}

enum SortOutcomeIter {
    Internal(std::vec::IntoIter<RecordData>),
    External(Run, Tape, std::vec::IntoIter<RecordData>),
}

impl From<SortOutcome> for SortOutcomeIter {
    fn from(value: SortOutcome) -> Self {
        match value {
            SortOutcome::Internal(inner) => SortOutcomeIter::Internal(inner.into_iter()),
            SortOutcome::External(run, tape) => {
                SortOutcomeIter::External(run, tape, Vec::new().into_iter())
            }
        }
    }
}
