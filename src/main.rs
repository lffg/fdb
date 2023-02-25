use std::path::Path;

use tracing::info;

use crate::{
    disk_manager::DiskManager,
    error::{DbResult, Error},
    page::{catalog_data::CatalogData, first::FirstPage, PageId, PageState},
    pager::Pager,
};

mod error;

mod catalog;
mod config;
mod page;

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
    dbg!(first_page.get());

    Ok(())
}

// TODO: While this database doesn't support user-defined tables (aka. `CREATE
// TABLE`), during bootstrap, one allocates a specific catalog to use for
// testing purposes.
fn define_test_catalog(pager: &mut Pager, first_page: &mut FirstPage) -> DbResult<()> {
    info!("defining test catalog");
    first_page.catalog = CatalogData {
        next_id: None,
        object_count: 1,
        objects: vec![catalog::Object {
            ty: catalog::ObjectType::Table,
            page: PageId::new(2.try_into().unwrap()),
            name: "chess_matches".into(),
        }],
    };
    pager.write_flush(first_page)?;
    tracing::warn!("todo finish this");
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
