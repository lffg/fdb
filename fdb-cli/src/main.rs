use std::{
    collections::HashMap,
    io::{self, Write},
    path::Path,
    str::FromStr,
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
    exec::{query, value::Value, values::Values},
    Db,
};
use tracing::instrument;

#[tokio::main]
async fn main() -> DbResult<()> {
    setup_tracing();

    let (db, first_access) = Db::open(Path::new("ignore/my-db")).await?;
    if first_access {
        define_test_catalog(&db).await?;
    }

    loop {
        println!("Pick a command: `insert`, `select`, `delete` or `quit`.");
        match &*input::<String>("cmd> ") {
            "insert" => {
                let id: i32 = input("id (int)> ");
                let name: String = input("name (text)> ");
                let age: i32 = input("age (int)> ");

                let insert_query = query::table::Insert::new(
                    "chess_matches",
                    Values::from(HashMap::from([
                        ("id".into(), Value::Int(id)),
                        ("name".into(), Value::Text(name)),
                        ("age".into(), Value::Int(age)),
                    ])),
                );

                db.execute(insert_query, |()| Ok::<_, ()>(()))
                    .await?
                    .unwrap();
                println!("ok");
            }
            "select" => {
                let select_query = query::table::Select::new("chess_matches");

                println!("{}", "-".repeat(50));
                db.execute(select_query, |row| {
                    let id = row.get("id").unwrap();
                    let name = row.get("name").unwrap();
                    let age = row.get("age").unwrap();
                    println!("{id:<4} | {name:<20} | {age:<4}");
                    Ok::<_, ()>(())
                })
                .await?
                .unwrap();
                println!("{}", "-".repeat(50));
            }
            "delete" => {
                let id: i32 = input("id (int)> ");
                let pred =
                    move |val: &Values| *val.get("id").unwrap().try_cast_int_ref().unwrap() == id;
                let del = query::table::Delete::new("chess_matches", &pred);
                db.execute(del, |_| Ok::<_, ()>(())).await?.unwrap();
                println!("ok");
            }
            "quit" => break,
            _ => {
                println!("invalid option; try again.");
            }
        }
    }

    Ok(())
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
#[instrument(skip_all)]
pub async fn define_test_catalog(db: &Db) -> DbResult<()> {
    let test_page_guard = db.pager().alloc::<HeapPage>().await?;
    let test_page = test_page_guard.write().await;

    let object = Object {
        ty: ObjectType::Table(get_chess_matches_schema()),
        // TODO: The page allocation should be encapsulated in the create object
        // implementation.
        page_id: test_page.id(),
        name: "chess_matches".into(),
    };

    let query = query::object::Create::new(&object);
    db.execute(query, |_| Ok::<(), ()>(())).await?.unwrap();

    test_page.flush();
    db.pager().flush_all().await?;

    Ok(())
}

fn get_chess_matches_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            Column {
                ty: TypeId::Primitive(PrimitiveTypeId::Int),
                name: "id".into(),
            },
            Column {
                ty: TypeId::Primitive(PrimitiveTypeId::Text),
                name: "name".into(),
            },
            Column {
                ty: TypeId::Primitive(PrimitiveTypeId::Int),
                name: "age".into(),
            },
        ],
    }
}
