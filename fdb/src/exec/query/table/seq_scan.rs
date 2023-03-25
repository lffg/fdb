use async_trait::async_trait;
use buff::Buff;
use tracing::instrument;

use crate::{
    catalog::{
        object::TableObject,
        record::simple_record::{SimpleRecord, TableRecordCtx},
        table_schema::TableSchema,
    },
    error::DbResult,
    exec::{
        operations::{heap, PhysicalState},
        query::Query,
        values::SchematizedValues,
    },
    util::io::DeserializeCtx,
    Db,
};

type Record = SimpleRecord<'static, SchematizedValues<'static>>;

/// A sequence scan query for tables.
pub struct SeqScan<'a> {
    table: &'a TableObject,
    seq_scan: heap::SeqScan<Record>,
}

#[async_trait]
impl Query for SeqScan<'_> {
    type Item<'a> = Record;

    #[instrument(name = "TableLinearScan", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        self.seq_scan
            .next(db, mk_deserializer(&self.table.schema))
            .await
    }
}

impl<'a> SeqScan<'a> {
    /// Creates a new insert executor.
    pub fn new(table: &'a TableObject) -> SeqScan<'a> {
        Self {
            table,
            seq_scan: heap::SeqScan::new(table.page_id),
        }
    }

    /// Returns the current element without advancing the underlying iterator.
    ///
    /// This method doesn't perform any kind of cache, which is handled by the
    /// underlying database pager.
    pub async fn _peek(&mut self, db: &Db) -> DbResult<Option<Record>> {
        self.seq_scan
            .peek(db, mk_deserializer(&self.table.schema))
            .await
    }
}

fn mk_deserializer(
    schema: &TableSchema,
) -> impl Fn(&mut Buff, PhysicalState) -> DbResult<Record> + '_ {
    |buf, state| {
        let ctx = TableRecordCtx::from_physical(state, schema);
        SimpleRecord::<SchematizedValues>::deserialize(buf, &ctx)
    }
}
