use std::collections::HashMap;

use fdb::{
    catalog::object::Object,
    error::DbResult,
    exec::{
        query::{self, table::OrderByDirection, Query},
        value::Value,
        values::Values,
    },
};
use tracing::{debug_span, Instrument};

mod test_utils;

#[tokio::test]
async fn test_sort() -> DbResult<()> {
    test_utils::setup_tracing(Some(r"trace"));

    const PAGE_SIZE: u16 = 128;

    let db = test_utils::TestDb::new_temp(Some(PAGE_SIZE)).await?;
    let table = Object::find(&db, "test_table").await?.try_into_table()?;

    let values: Vec<_> = (0..64)
        .map(|i| {
            Values::from(HashMap::from([
                ("id".into(), Value::Int(i + 1)),
                ("text".into(), Value::Text(format!("{:0>8}", i + 1))),
                ("bool".into(), Value::Bool(true)),
            ]))
        })
        .collect();

    async {
        for value in values.iter() {
            let ins = query::table::Insert::new(&table, value.clone());
            db.execute(ins, |_| Ok::<_, ()>(())).await?.unwrap();
        }
        Ok::<_, fdb::error::Error>(())
    }
    .instrument(debug_span!("inserting"))
    .await?;

    let mut sort = query::table::Sort::new(
        &table,
        query::table::SortOpts {
            mem_pages_limit: 5,
            order_by: &[("id", OrderByDirection::Desc)],
            ways: 2,
        },
    );
    sort.next(&*db).await?;

    Ok(())
}
