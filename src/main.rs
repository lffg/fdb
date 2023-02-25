use std::path::Path;

use tracing::info;

use crate::{
    disk_manager::DiskManager,
    error::{DbResult, Error},
    page::{first::FirstPage, PageId},
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

    let first_page = load_first_page(&mut pager)?;
    dbg!(first_page);

    Ok(())
}

/// Loads the first page, or bootstraps it in the case of first access.
fn load_first_page(pager: &mut Pager) -> DbResult<FirstPage> {
    let id = PageId::new(1.try_into().unwrap());

    match pager.load(id) {
        Ok(first_page) => Ok(first_page),
        Err(Error::PageOutOfBounds(_)) => {
            info!("first access; bootstrapping first page");
            let first_page = FirstPage::default();
            pager.write_flush(&first_page)?;
            Ok(first_page)
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
