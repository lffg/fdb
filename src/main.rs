use std::path::Path;

use tracing::info;

use crate::{
    catalog::{
        object::{Object, ObjectSchema, ObjectType},
        page::{FirstPage, HeapPage, PageId, PageState},
    },
    disk_manager::DiskManager,
    error::{DbResult, Error},
    pager::Pager,
};

mod error;

mod catalog;
mod config;

mod disk_manager;
mod pager;

mod ioutil;

fn main() -> DbResult<()> {
    setup_tracing();

    let disk_manager = DiskManager::new(Path::new("ignore/my-db"))?;
    let mut pager = Pager::new(disk_manager);

    let mut first_page = load_first_page(&mut pager)?;
    if let PageState::New(first_page) = &mut first_page {
        define_test_catalog(&mut pager, first_page)?;
    };
    // TODO: Load full object catalog.

    let mut second_page: HeapPage = pager.load(PageId::new_u32(2))?;

    println!("First page:\n{:#?}\n", first_page.get());
    second_page.bytes = vec![]; // hide for print below.
    println!("Second page:\n{second_page:#?}\n");

    Ok(())
}

// TODO: While this database doesn't support user-defined tables (aka. `CREATE
// TABLE`), during bootstrap, one allocates a specific catalog to use for
// testing purposes.
fn define_test_catalog(pager: &mut Pager, first_page: &mut FirstPage) -> DbResult<()> {
    info!("defining test catalog");

    let heap_page_id = PageId::new_u32(2);

    first_page.object_schema = ObjectSchema {
        next_id: None,
        object_count: 1,
        objects: vec![Object {
            ty: ObjectType::Table,
            page_id: heap_page_id,
            name: "chess_matches".into(),
        }],
    };
    pager.write_flush(first_page)?;

    let first_heap_page = HeapPage {
        id: heap_page_id,
        next_page_id: None,
        bytes: b"hello, world! (i am not yet structured)".to_vec(),
    };
    pager.write_flush(&first_heap_page)?;

    Ok(())
}

/// Loads the first page, or bootstraps it in the case of first access.
fn load_first_page(pager: &mut Pager) -> DbResult<PageState<FirstPage>> {
    let id = PageId::new(1.try_into().unwrap());

    match pager.load(id) {
        Ok(first_page) => Ok(PageState::Existing(first_page)),
        Err(Error::PageOutOfBounds(_)) => {
            info!("first access; bootstrapping first page");
            let first_page = FirstPage::default();
            pager.write_flush(&first_page)?;
            Ok(PageState::New(first_page))
        }
        Err(Error::ReadIncompletePage(_)) => {
            panic!("corrupt database file");
        }
        Err(error) => Err(error),
    }
}

/// Sets up tracing subscriber.
fn setup_tracing() {
    use tracing_subscriber::{
        fmt::{format::FmtSpan, layer},
        layer::SubscriberExt,
        util::SubscriberInitExt,
        EnvFilter,
    };

    let filter_layer = EnvFilter::try_from_default_env().unwrap_or("warn".into());
    let fmt_layer = layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();
}
