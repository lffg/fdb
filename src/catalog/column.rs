use crate::{
    catalog::ty::TypeId,
    error::DbResult,
    ioutil::{Serde, VarString},
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

impl Serde<'_> for Column {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.ty.serialize(buf)?;
        VarString::from(self.name.as_str()).serialize(buf)?;
        Ok(())
    }

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
