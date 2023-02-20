use bytes::{Buf, BufMut};

pub trait Type: Sized {
    type Error;

    fn type_name() -> &'static str;
    fn size(&self) -> TypeSize;

    fn serialize(&self, out: &mut dyn BufMut) -> Result<(), Self::Error>;
    fn deserialize(buf: &mut dyn Buf) -> Result<Self, Self::Error>;
}

pub enum TypeSize {
    Fixed(usize),
    Varying(usize),
}

pub mod types {
    use super::*;

    struct Number<T>(T);

    pub type Byte = Number<u8>;

    pub type ShortInt = Number<i16>;
    pub type Int = Number<i32>;
    pub type BigInt = Number<i64>;
    pub type Float = Number<f64>;

    macro_rules! impl_type {
        ($($name:ident::<$type:ty>($get:ident, $put:ident),)+) => {
            $(
                impl Type for Number<$type> {
                    type Error = std::convert::Infallible;

                    fn type_name() -> &'static str {
                        stringify!($name)
                    }

                    fn size(&self) -> TypeSize {
                        TypeSize::Fixed(std::mem::size_of::<$type>())
                    }

                    fn serialize(&self, out: &mut dyn BufMut) -> Result<(), Self::Error> {
                        out.$put(self.0);
                        Ok(())
                    }

                    fn deserialize(buf: &mut dyn Buf) -> Result<Self, Self::Error> {
                        Ok(Self(buf.$get()))
                    }
                }
            )+
        };
    }

    impl_type![
        byte::<u8>(get_u8, put_u8),
        shortint::<i16>(get_i16, put_i16),
        int::<i32>(get_i32, put_i32),
    ];
}
