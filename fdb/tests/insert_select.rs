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

    let values = &[
        Values::from(HashMap::from([
            ("id".into(), Value::Int(1)),
            ("text".into(), Value::Text("hello, world!".into())),
            ("bool".into(), Value::Bool(true)),
        ])),
        Values::from(HashMap::from([
            ("id".into(), Value::Int(2)),
            ("text".into(), Value::Text("olá, mundo!".into())),
            ("bool".into(), Value::Bool(false)),
        ])),
        Values::from(HashMap::from([
            ("id".into(), Value::Int(3)),
            ("text".into(), Value::Text("a".to_string().repeat(3000))),
            ("bool".into(), Value::Bool(true)),
        ])),
        Values::from(HashMap::from([
            ("id".into(), Value::Int(4)),
            ("text".into(), Value::Text("b".to_string().repeat(3000))),
            ("bool".into(), Value::Bool(true)),
        ])),
    ];

    {
        for value in values.iter() {
            let ins = query::table::Insert::new("test_table", value.clone());
            db.execute(ins, |_| Ok::<_, ()>(())).await?.unwrap();
        }
    }

    {
        let mut expected_rows: HashMap<_, _> = values
            .into_iter()
            .map(|value| (*value.get("id").unwrap().try_cast_int_ref().unwrap(), value))
            .collect();
        let second_select = query::table::Select::new("test_table");
        db.execute(second_select, |row| {
            let expected = expected_rows
                .remove(row.get("id").unwrap().try_cast_int_ref().unwrap())
                .unwrap();
            assert_eq!(&row, expected);
            Ok::<_, ()>(())
        })
        .await?
        .unwrap();
        assert_eq!(expected_rows.len(), 0);
    }

    Ok(())
}
