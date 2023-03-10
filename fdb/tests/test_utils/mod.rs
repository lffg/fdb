use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};

use fdb::{
    catalog::{
        column::Column,
        object::{Object, ObjectType},
        page::{HeapPage, SpecificPage},
        table_schema::TableSchema,
        ty::{PrimitiveTypeId, TypeId},
    },
    error::DbResult,
    exec::query,
    Db,
};
use tokio::fs;

/// Sets up tracing subscriber.
#[allow(dead_code)]
pub fn setup_tracing(level: Option<&str>) {
    use tracing_subscriber::{
        fmt::{format::FmtSpan, layer},
        layer::SubscriberExt,
        util::SubscriberInitExt,
        EnvFilter,
    };

    let filter_layer = level
        .map(EnvFilter::new)
        .unwrap_or_else(|| EnvFilter::try_from_default_env().unwrap_or("warn".into()));
    let fmt_layer = layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();
}

pub struct TestDb(Db, PathBuf);

impl TestDb {
    /// Creates a new test database in a temporary file.
    pub async fn new_temp() -> DbResult<Self> {
        let path = test_path().await;

        let (db, is_new) = Db::open(&path).await?;
        assert!(is_new, "db file must be new");
        define_test_catalog(&db).await?;

        Ok(Self(db, path))
    }
}

impl Deref for TestDb {
    type Target = Db;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TestDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        std::fs::remove_file(&self.1).unwrap();
    }
}

/// Generates a path to the test database.
async fn test_path() -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(1);

    let id = COUNTER.fetch_add(1, Ordering::AcqRel);
    fs::create_dir_all("ignore").await.unwrap();
    PathBuf::from(format!("ignore/{id}-test.db"))
}

// TODO: Remove me.
pub async fn define_test_catalog(db: &Db) -> DbResult<()> {
    let test_page_guard = db.pager().alloc(HeapPage::new_seq_first).await?;
    let test_page = test_page_guard.write().await;

    let object = Object {
        ty: ObjectType::Table(get_test_schema()),
        page_id: test_page.id(),
        name: "test_table".into(),
    };

    let query = query::object::Create::new(&object);
    db.execute(query, |_| Ok::<(), ()>(())).await?.unwrap();

    test_page.flush();
    db.pager().flush_all().await?;

    Ok(())
}

fn get_test_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            Column {
                ty: TypeId::Primitive(PrimitiveTypeId::Int),
                name: "id".into(),
            },
            Column {
                ty: TypeId::Primitive(PrimitiveTypeId::Text),
                name: "text".into(),
            },
            Column {
                ty: TypeId::Primitive(PrimitiveTypeId::Bool),
                name: "bool".into(),
            },
        ],
    }
}
