use crate::AsBytes;

macro_rules! number_impls {
    ($($type:ty),+) => {
        $(
            impl crate::AsBytes for $type {
                type Repr = [u8; ::std::mem::size_of::<$type>()];

                fn serialize(&self) -> Self::Repr {
                    self.to_be_bytes()
                }

                fn deserialize(src: Self::Repr) -> Self {
                    Self::from_be_bytes(src)
                }
            }
        )+
    }
}

number_impls![u8, u16, u32, u64, i8, i16, i32, i64, f32, f64];

impl AsBytes for bool {
    type Repr = [u8; 1];

    fn serialize(&self) -> Self::Repr {
        [u8::from(*self)]
    }

    fn deserialize(src: Self::Repr) -> Self {
        match src {
            [0] => false,
            [1] => true,
            _ => panic!("deserialization error: invalid bool representation"),
        }
    }
}
