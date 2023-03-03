use crate::{catalog::column::Column, error::DbResult, ioutil::Serde};

/// A table object schema.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// The column count.
    pub column_count: u16,
    /// The table columns.
    ///
    /// This in-memory vector is assumed to be in the same order as the fields
    /// are represented on the disk.
    pub columns: Vec<Column>,
}

impl Serde<'_> for TableSchema {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write(self.column_count);
        debug_assert_eq!(self.column_count as usize, self.columns.len());
        for column in &self.columns {
            column.serialize(buf)?;
        }
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let column_count: u16 = buf.read();
        let columns: Vec<_> = (0..column_count)
            .map(|_| Column::deserialize(buf))
            .collect::<Result<_, _>>()?;
        Ok(TableSchema {
            column_count,
            columns,
        })
    }
}
