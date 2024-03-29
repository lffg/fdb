use crate::{
    catalog::column::Column,
    error::DbResult,
    util::io::{Deserialize, Serialize, Size, VarList},
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

impl Size for TableSchema {
    fn size(&self) -> u32 {
        VarList::from(self.columns.as_slice()).size()
    }
}

impl Serialize for TableSchema {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        VarList::from(self.columns.as_slice()).serialize(buf)?;
        Ok(())
    }
}

impl Deserialize<'_> for TableSchema {
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(TableSchema {
            columns: VarList::deserialize(buf)?.into(),
        })
    }
}
