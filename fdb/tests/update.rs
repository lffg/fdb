use std::collections::HashMap;

use fdb::{
    catalog::object::Object,
    error::DbResult,
    exec::{query, value::Value, values::Values},
};

mod test_utils;

#[tokio::test]
async fn test_update_smaller() -> DbResult<()> {
    let db = test_utils::TestDb::new_temp().await?;
    let table = Object::find(&db, "test_table").await?.try_into_table()?;

    let values = &mut [
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
            let ins = query::table::Insert::new(&table, value.clone());
            db.execute(ins, |_| Ok::<_, ()>(())).await?.unwrap();
        }
    }

    {
        let pred = |val: &Values| *val.get("id").unwrap().try_cast_int_ref().unwrap() == 2;
        let updater = |val: &mut Values| val.set("text".into(), Value::Text("olá!".into()));
        let del = query::table::Update::new(&table, &pred, &updater);
        db.execute(del, |_| Ok::<_, ()>(())).await?.unwrap();
    }

    {
        values[1].set("text".into(), Value::Text("olá!".into()));
        let mut expected_rows: HashMap<_, _> = values
            .into_iter()
            .map(|value| (*value.get("id").unwrap().try_cast_int_ref().unwrap(), value))
            .collect();
        let second_select = query::table::Select::new(&table);
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

#[tokio::test]
async fn test_update_bigger() -> DbResult<()> {
    let db = test_utils::TestDb::new_temp().await?;
    let table = Object::find(&db, "test_table").await?.try_into_table()?;

    let values = &mut [
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
            let ins = query::table::Insert::new(&table, value.clone());
            db.execute(ins, |_| Ok::<_, ()>(())).await?.unwrap();
        }
    }

    const NEW_TEXT: &str = "olá, mundo! olá, mundo! :))";

    {
        let pred = |val: &Values| *val.get("id").unwrap().try_cast_int_ref().unwrap() == 2;
        let updater = |val: &mut Values| val.set("text".into(), Value::Text(NEW_TEXT.into()));
        let del = query::table::Update::new(&table, &pred, &updater);
        db.execute(del, |_| Ok::<_, ()>(())).await?.unwrap();
    }

    {
        values[1].set("text".into(), Value::Text(NEW_TEXT.into()));
        let mut expected_rows: HashMap<_, _> = values
            .into_iter()
            .map(|value| (*value.get("id").unwrap().try_cast_int_ref().unwrap(), value))
            .collect();
        let second_select = query::table::Select::new(&table);
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
