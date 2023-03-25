use async_trait::async_trait;
use buff::Buff;
use tracing::instrument;

use crate::{
    catalog::{
        object::Object,
        page::PageId,
        record::simple_record::{SimpleCtx, SimpleRecord},
    },
    error::DbResult,
    exec::{
        operations::{heap, PhysicalState},
        query::Query,
    },
    util::io::DeserializeCtx,
    Db,
};

const FIRST_SCHEMA_PAGE_ID: PageId = PageId::new_u32(2);

type ObjectRecord = SimpleRecord<'static, Object>;

/// An object selection query.
pub struct Select {
    seq_scan: heap::SeqScan<ObjectRecord>,
}

#[async_trait]
impl Query for Select {
    type Item<'a> = Object;

    #[instrument(name = "ObjectSelect", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        loop {
            return match self.seq_scan.next(db, deserializer).await? {
                Some(record) => {
                    if record.is_deleted() {
                        continue;
                    }
                    Ok(Some(record.into_data().into_owned()))
                }
                None => Ok(None),
            };
        }
    }
}

impl Select {
    pub fn new() -> Select {
        Self {
            seq_scan: heap::SeqScan::new(FIRST_SCHEMA_PAGE_ID),
        }
    }
}

impl Default for Select {
    fn default() -> Self {
        Self::new()
    }
}

fn deserializer(buf: &mut Buff<'_>, state: PhysicalState) -> DbResult<ObjectRecord> {
    let ctx = SimpleCtx::from_physical(state);
    ObjectRecord::deserialize(buf, &ctx)
}
