use std::{
    collections::HashMap,
    io::{self, Write},
    path::Path,
    str::FromStr,
};

use tracing::info;

use crate::{
    catalog::{
        column::Column,
        object::{Object, ObjectSchema, ObjectType},
        page::{FirstHeapPage, FirstPage, PageId, PageState},
        table_schema::TableSchema,
        ty::TypeId,
    },
    disk_manager::DiskManager,
    error::{DbResult, Error},
    exec::{
        value::{Environment, Value},
        ExecCtx, Executor,
    },
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
        define_test_catalog(&mut pager, first_page)?;
    };

    loop {
        let mut exec_ctx = ExecCtx {
            pager: &mut pager,
            object_schema: &first_page.get().object_schema,
        };

        println!("Pick a command: `insert`, `select` or `quit`.");
        match &*input::<String>("cmd> ") {
            "insert" => {
                let id: i32 = input("id (int)> ");
                let name: String = input("name (text)> ");
                let age: i32 = input("age (int)> ");
                let mut cmd = exec::Insert::new(
                    "chess_matches",
                    Environment::from(HashMap::from([
                        ("id".into(), Value::Int(id)),
                        ("name".into(), Value::Text(name)),
                        ("age".into(), Value::Int(age)),
                    ])),
                );
                cmd.next(&mut exec_ctx)?;
                println!("ok");
            }
            "select" => {
                let mut cmd = exec::Select::new("chess_matches");
                println!("{}", "-".repeat(50));
                while let Some(env) = cmd.next(&mut exec_ctx)? {
                    // Skip logically deleted rows.
                    let Some(row) = env else { continue };

                    let id = row.get("id").unwrap();
                    let name = row.get("name").unwrap();
                    let age = row.get("age").unwrap();
                    println!("{id:<4} | {name:<20} | {age:<4}");
                }
                println!("{}", "-".repeat(50));
            }
            "quit" => break,
            _ => {
                println!("invalid option; try again.");
            }
        }
    }

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

/// Gets a value from the stdin.
fn input<T: FromStr>(prompt: &str) -> T {
    print!("{prompt}");
    io::stdout().flush().unwrap();
    let mut buf = String::new();
    loop {
        io::stdin().read_line(&mut buf).unwrap();
        match T::from_str(buf.trim()) {
            Ok(val) => break val,
            Err(_) => println!("try again"),
        }
    }
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
        column_count: 3,
        columns: vec![
            Column {
                ty: TypeId::Int,
                name: "id".into(),
            },
            Column {
                ty: TypeId::Text,
                name: "name".into(),
            },
            Column {
                ty: TypeId::Int,
                name: "age".into(),
            },
        ],
    }
}
