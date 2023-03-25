use crate::{
    catalog::ty::TypeId,
    error::DbResult,
    util::io::{Deserialize, Serialize, Size, VarString},
};

/// A column definition.
#[derive(Debug, Clone)]
pub struct Column {
    /// The column value type.
    pub ty: TypeId,
    /// The column identifier.
    ///
    /// The column name may have at most 64 bytes.
    pub name: String,
}

impl Size for Column {
    fn size(&self) -> u32 {
        self.ty.size() + VarString::from(self.name.as_str()).size()
    }
}

impl Serialize for Column {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.ty.serialize(buf)?;
        VarString::from(self.name.as_str()).serialize(buf)?;
        Ok(())
    }
}

impl Deserialize<'_> for Column {
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(Column {
            ty: TypeId::deserialize(buf)?,
            name: VarString::deserialize(buf)?.into(),
        })
    }
}
