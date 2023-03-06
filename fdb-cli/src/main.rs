use std::{
    collections::HashMap,
    io::{self, Write},
    path::Path,
    str::FromStr,
};

use fdb::{
    catalog::{
        column::Column,
        object::{Object, ObjectSchema, ObjectType},
        page::{FirstPage, HeapPage, PageId, SpecificPage},
        table_schema::TableSchema,
        ty::TypeId,
    },
    error::{DbResult, Error},
    exec::{
        self,
        value::{Environment, Value},
        ExecCtx, Executor,
    },
    io::{
        disk_manager::DiskManager,
        pager::{Pager, PagerGuard},
    },
};
use tracing::info;

#[tokio::main]
async fn main() -> DbResult<()> {
    setup_tracing();

    let disk_manager = DiskManager::new(Path::new("ignore/my-db")).await?;
    let mut pager = Pager::new(disk_manager);

    let (first_page_guard, is_new) = boot_first_page(&mut pager).await?;
    if is_new {
        define_test_catalog(&pager).await?;
    }

    info!("getting schema...");
    let first_page = first_page_guard.read().await;
    let schema = first_page.object_schema.clone();
    first_page.release();

    loop {
        let exec_ctx = ExecCtx {
            pager: &mut pager,
            object_schema: &schema,
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
                cmd.next(&exec_ctx).await?;
                println!("ok");
            }
            "select" => {
                let mut cmd = exec::Select::new("chess_matches");
                println!("{}", "-".repeat(50));
                while let Some(env) = cmd.next(&exec_ctx).await? {
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
///
/// It also returns a boolean that, if true, indicates that the page was booted
/// for the first time.
async fn boot_first_page(pager: &mut Pager) -> DbResult<(PagerGuard<FirstPage>, bool)> {
    match pager.get::<FirstPage>(PageId::FIRST).await {
        Ok(guard) => Ok((guard, false)),
        Err(Error::PageOutOfBounds(_)) => {
            info!("first access; booting first page");

            let first_page = FirstPage::default_with_id(PageId::FIRST);
            // SAFETY: This is the first page, no metadata is needed, yet.
            unsafe {
                pager.clear_cache(PageId::FIRST).await;
                pager.flush_page(&first_page).await?;
            };

            Ok((pager.get::<FirstPage>(PageId::FIRST).await?, true))
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
    let mut buf = String::new();
    loop {
        print!("{prompt}");
        io::stdout().flush().unwrap();
        buf.clear();
        if io::stdin().read_line(&mut buf).unwrap() == 0 {
            println!("\nbye");
            std::process::exit(0);
        }
        match T::from_str(buf.trim()) {
            Ok(val) => break val,
            Err(_) => println!("try again."),
        }
    }
}

// TODO: While this database doesn't support user-defined tables (aka. `CREATE
// TABLE`), during bootstrap, one allocates a specific catalog to use for
// testing purposes.
pub async fn define_test_catalog(pager: &Pager) -> DbResult<()> {
    info!("defining test catalog");

    let seq_first_guard = pager.alloc::<HeapPage>().await?;
    let seq_first = seq_first_guard.write().await;

    let first_page_guard = pager.get::<FirstPage>(PageId::FIRST).await?;
    let mut first_page = first_page_guard.write().await;

    first_page.object_schema = ObjectSchema {
        next_id: None,
        objects: vec![Object {
            ty: ObjectType::Table(get_chess_matches_schema()),
            page_id: seq_first.id(),
            name: "chess_matches".into(),
        }],
    };

    first_page.flush();
    seq_first.flush();

    pager.flush_all().await?;

    Ok(())
}

fn get_chess_matches_schema() -> TableSchema {
    TableSchema {
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
