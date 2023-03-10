use std::collections::HashMap;

use fdb::{
    error::DbResult,
    exec::{query, value::Value, values::Values},
};

mod test_utils;

#[tokio::test]
async fn test_delete() -> DbResult<()> {
    let db = test_utils::TestDb::new_temp().await?;

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
            ("text".into(), Value::Text("woo!".into())),
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
        let del = query::table::Delete::new("test_table", &|val| {
            *val.get("id").unwrap().try_cast_int_ref().unwrap() == 2
        });
        db.execute(del, |_| Ok::<_, ()>(())).await?.unwrap();
    }

    {
        let mut expected_rows: HashMap<_, _> = values
            .into_iter()
            .map(|value| (*value.get("id").unwrap().try_cast_int_ref().unwrap(), value))
            .filter(|(id, _)| *id != 2)
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
