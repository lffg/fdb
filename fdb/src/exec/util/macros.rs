macro_rules! seq_h {
    (mut $guard:expr) => {
        $guard.header.seq_header.as_mut().expect("first page")
    };
    ($guard:expr) => {
        $guard.header.seq_header.as_ref().expect("first page")
    };
}
pub(crate) use seq_h;
