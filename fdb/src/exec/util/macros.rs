macro_rules! seq_h {
    (mut $guard:expr) => {
        $guard.header.seq_header.as_mut().expect("first page")
    };
    ($guard:expr) => {
        $guard.header.seq_header.as_ref().expect("first page")
    };
}
pub(crate) use seq_h;

macro_rules! get_or_insert_with {
    ($opt:expr, || $($init:tt)*) => {
        if let Some(inner) = $opt {
            inner
        } else {
            let init_val = {
                $($init)*
            };
            $opt.insert(init_val)
        }
    }
}
pub(crate) use get_or_insert_with;
