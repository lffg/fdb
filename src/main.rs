use std::path::Path;

use tracing::info;

use crate::{
    catalog::page::{FirstPage, PageId, PageState},
    disk_manager::DiskManager,
    error::{DbResult, Error},
    pager::Pager,
};

mod error;

mod catalog;
mod config;

mod disk_manager;
mod pager;

mod exec;

mod ioutil;

fn main() -> DbResult<()> {
    setup_tracing();

    let disk_manager = DiskManager::new(Path::new("ignore/my-db"))?;
    let mut pager = Pager::new(disk_manager);

    let mut first_page = load_first_page(&mut pager)?;
    if let PageState::New(first_page) = &mut first_page {
        t::define_test_catalog(&mut pager, first_page)?;
    };
    // TODO: Load full object catalog.

    t::main(&mut pager)?;

    Ok(())
}

/// Loads the first page, or bootstraps it in the case of first access.
fn load_first_page(pager: &mut Pager) -> DbResult<PageState<FirstPage>> {
    let id = PageId::new(1.try_into().unwrap());

    match pager.load(id) {
        Ok(first_page) => Ok(PageState::Existing(first_page)),
        Err(Error::PageOutOfBounds(_)) => {
            info!("first access; bootstrapping first page");
            let first_page = FirstPage::new();
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

/// Testing utilities. This will be removed.
mod t {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        catalog::{
            column::Column,
            object::{Object, ObjectSchema, ObjectType},
            page::{FirstHeapPage, FirstPage, PageId},
            table_schema::TableSchema,
            ty::TypeId,
        },
        error::DbResult,
        exec::{
            value::{Environment, Value},
            Command, ExecCtx,
        },
        pager::Pager,
    };

    pub fn main(pager: &mut Pager) -> DbResult<()> {
        println!("=== after initialization ===");
        print_pages(pager)?;

        println!("\n\n");

        // obviously this is not permanent.
        let first_page: FirstPage = pager.load(PageId::new_u32(1))?;

        let cmd = exec::InsertCmd {
            table_name: "chess_matches",
            env: Environment::from(HashMap::from([
                ("id".into(), Value::Int(4)),
                ("age".into(), Value::Int(0xF)),
            ])),
        };
        cmd.execute(&mut ExecCtx {
            pager,
            object_schema: &first_page.object_schema,
        })?;

        println!("=== after insert ===");
        print_pages(pager)?;

        Ok(())
    }

    fn print_pages(pager: &mut Pager) -> DbResult<()> {
        let first_page: FirstPage = pager.load(PageId::new_u32(1))?;
        let mut second_page: FirstHeapPage = pager.load(PageId::new_u32(2))?;

        println!("First page:\n{first_page:#?}\n");
        second_page.ordinary_page.bytes = vec![]; // hide for print below.
        println!("Second page:\n{second_page:#?}\n");

        Ok(())
    }

    // TODO: While this database doesn't support user-defined tables (aka. `CREATE
    // TABLE`), during bootstrap, one allocates a specific catalog to use for
    // testing purposes.
    pub fn define_test_catalog(pager: &mut Pager, first_page: &mut FirstPage) -> DbResult<()> {
        info!("defining test catalog");

        let first_chess_matches_page_id = PageId::new_u32(2);

        first_page.object_schema = ObjectSchema {
            next_id: None,
            object_count: 1,
            objects: vec![Object {
                ty: ObjectType::Table(get_chess_matches_schema()),
                page_id: first_chess_matches_page_id,
                name: "chess_matches".into(),
            }],
        };
        pager.write_flush(first_page)?;

        let first_chess_matches_table = FirstHeapPage::new(first_chess_matches_page_id);

        pager.write_flush(&first_chess_matches_table)?;

        Ok(())
    }

    fn get_chess_matches_schema() -> TableSchema {
        TableSchema {
            column_count: 2,
            columns: vec![
                Column {
                    ty: TypeId::Int,
                    name: "id".into(),
                },
                Column {
                    ty: TypeId::Int,
                    name: "age".into(),
                },
            ],
        }
    }
}
