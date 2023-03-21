use std::cmp::Ordering;

use crate::{
    catalog::object::TableObject,
    error::{DbResult, Error},
    exec::values::Values,
};

/// A list of pairs of a column name and a [OrderByDirection].
pub type OrderBy<'a> = &'a [(&'a str, OrderByDirection)];

/// Order by direction.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OrderByDirection {
    Asc,
    Desc,
}

/// A dynamically-typed function that compares two [`Values`] objects.
pub type CmpFn<'a> = dyn Send + Sync + 'a + for<'v> Fn(&'v Values, &'v Values) -> Ordering;

/// Builds the cmp function and stores it in a box.
pub fn new_boxed_cmp_fn<'a>(
    table: &'a TableObject,
    order_by: OrderBy<'a>,
) -> DbResult<Box<CmpFn<'a>>> {
    if order_by.len() != 1 {
        return Err(Error::ExecError(
            "fdb currently only supports order by with one field".into(),
        ));
    }

    for (col_name, _) in order_by {
        if table.schema.column(col_name).is_none() {
            return Err(Error::ExecError(format!(
                "column `{col_name}` doesn't exist on table {}",
                table.name
            )));
        }
    }

    Ok(Box::new(|a: &Values, b: &Values| -> Ordering {
        let col_name = order_by[0].0;
        let direction = order_by[0].1;

        // SAFETY: Column exists (as checked above).
        let a_val = unsafe { a.get(col_name).unwrap_unchecked() };
        let b_val = unsafe { b.get(col_name).unwrap_unchecked() };

        let cmp_result = a_val.cmp(&b_val);

        if direction == OrderByDirection::Asc {
            cmp_result
        } else {
            cmp_result.reverse()
        }
    }))
}
