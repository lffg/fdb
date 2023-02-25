use crate::{catalog, error::DbResult, ioutil::Serde};

#[derive(Debug)]
pub struct SchemaData {
    pub column_count: u16,
    pub columns: Vec<catalog::Column>,
}

impl Serde for SchemaData {
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
            .map(|_| catalog::Column::deserialize(buf))
            .collect::<Result<_, _>>()?;
        Ok(SchemaData {
            column_count,
            columns,
        })
    }
}
