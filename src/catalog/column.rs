use crate::{
    catalog::ty::TypeId,
    error::DbResult,
    ioutil::{BuffExt, Serde},
};

/// A column definition.
#[derive(Debug)]
pub struct Column {
    /// The column value type.
    pub ty: TypeId,
    /// The column identifier.
    ///
    /// The column name may have at most 64 bytes.
    pub name: String,
}

impl Serde for Column {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        self.ty.serialize(buf)?;
        buf.write_var_size_string(&self.name)?;
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(Column {
            ty: TypeId::deserialize(buf)?,
            name: buf.read_var_size_string()?,
        })
    }
}
