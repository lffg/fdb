use crate::catalog;

#[derive(Debug)]
pub struct SchemaData {
    pub column_count: u16,
    pub columns: Vec<catalog::Column>,
}
