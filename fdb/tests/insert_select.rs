use std::collections::HashMap;

use fdb::{
    error::DbResult,
    exec::{query, value::Value, values::Values},
};

mod test_utils;

#[tokio::test]
async fn test_insert_select() -> DbResult<()> {
    // test_utils::setup_tracing(Some("debug"));

    let db = test_utils::TestDb::new_temp().await?;

    {
        let first_select = query::table::Select::new("test_table");
        db.execute::<_, _, ()>(first_select, |_| {
            panic!("should be empty");
        })
        .await?
        .unwrap();
    }

    let values_1 = Values::from(HashMap::from([
        ("id".into(), Value::Int(1)),
        ("text".into(), Value::Text("hello, world!".into())),
        ("bool".into(), Value::Bool(true)),
    ]));
    let values_2 = Values::from(HashMap::from([
        ("id".into(), Value::Int(2)),
        ("text".into(), Value::Text("olá, mundo!".into())),
        ("bool".into(), Value::Bool(false)),
    ]));

    {
        let first_insert = query::table::Insert::new("test_table", values_1.clone());
        db.execute(first_insert, |_| Ok::<_, ()>(()))
            .await?
            .unwrap();
        let second_insert = query::table::Insert::new("test_table", values_2.clone());
        db.execute(second_insert, |_| Ok::<_, ()>(()))
            .await?
            .unwrap();
    }

    {
        let mut expected_rows = HashMap::from([(1, values_1), (2, values_2)]);
        let second_select = query::table::Select::new("test_table");
        db.execute(second_select, |row| {
            let expected = expected_rows
                .remove(row.get("id").unwrap().try_cast_int_ref().unwrap())
                .unwrap();
            assert_eq!(row, expected);
            Ok::<_, ()>(())
        })
        .await?
        .unwrap();
        assert_eq!(expected_rows.len(), 0);
    }

    Ok(())
}
