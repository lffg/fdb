use crate::{
    catalog::column::Column,
    error::DbResult,
    util::io::{Serde, Size, VarList},
};

/// A table schema.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// The table columns.
    ///
    /// This in-memory vector is assumed to be in the same order as the fields
    /// are represented on the disk.
    pub columns: Vec<Column>,
}

impl TableSchema {
    /// Checks if the schema contains the given column, returning a reference to
    /// it.
    ///
    /// This is a linear operation which, in the worst case, scans over all of
    /// the `schema` columns.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|col| col.name == name)
    }
}

impl Size for TableSchema {
    fn size(&self) -> u32 {
        VarList::from(self.columns.as_slice()).size()
    }
}

impl Serde<'_> for TableSchema {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        VarList::from(self.columns.as_slice()).serialize(buf)?;
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(TableSchema {
            columns: VarList::deserialize(buf)?.into(),
        })
    }
}
