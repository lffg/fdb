use std::fmt;

mod impls;

/// Represents a type that may be serialized to bytes and deserialized from
/// bytes.
pub trait AsBytes: Sized {
    /// The serialized representation.
    type Repr;

    /// Serializes the type to its byte representation.
    fn serialize(&self) -> Self::Repr;

    /// Deserializes the byte representation to its corresponding type.
    fn deserialize(src: Self::Repr) -> Self;
}

/// A fixed-size buffer (buff, aka. buf fixed).
///
/// # Panics
///
/// All `put_*` methods panic if there is not enough capacity.
pub struct Buff<'a> {
    inner: &'a mut [u8],
    offset: usize,
}

impl<'a> Buff<'a> {
    /// Creates a new fixed-size buffer, `Buff`.
    pub fn new(inner: &'a mut [u8]) -> Buff<'a> {
        Buff { inner, offset: 0 }
    }

    /// Returns the underlying buffer.
    pub fn get(&self) -> &[u8] {
        self.inner
    }

    /// Returns a mutable reference to the underlying buffer.
    pub fn get_mut(&mut self) -> &mut [u8] {
        self.inner
    }

    /// Returns the buffer capacity.
    pub fn capacity(&self) -> usize {
        self.inner.len()
    }

    /// Returns the remaining available bytes in the buffer.
    pub fn remaining(&self) -> usize {
        self.capacity() - self.offset
    }

    /// Returns the current offset.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Changes the underlying cursor offset position.
    pub fn seek(&mut self, offset: usize) {
        self.offset = offset;
    }

    /// Reads the type represented by [`AsBytes`].
    pub fn read<const S: usize, T>(&mut self) -> T
    where
        T: AsBytes<Repr = [u8; S]>,
    {
        let mut buf = [0; S]; // TODO: Optimize using `MaybeUninit`.
        self.read_slice(&mut buf);
        T::deserialize(buf)
    }

    /// Reads exactly the amount of bytes necessary to fill the given slice.
    pub fn read_slice(&mut self, dest: &mut [u8]) {
        dest.copy_from_slice(self.slice_to(dest.len()));
    }

    /// Writes the type represented by [`AsBytes`].
    pub fn write<T>(&mut self, src: T)
    where
        T: AsBytes,
        T::Repr: AsRef<[u8]>,
    {
        let data = src.serialize();
        self.write_slice(data.as_ref());
    }

    /// Writes the byte sequence into the buffer, starting at the current
    /// length.
    pub fn write_slice(&mut self, src: &[u8]) {
        self.slice_to(src.len()).copy_from_slice(src);
    }

    /// Writes `count` times the given byte.
    pub fn write_bytes(&mut self, count: usize, val: u8) {
        self.slice_to(count).fill(val);
    }

    /// Creates a scope used to compute the byte delta.
    pub fn delta<F, R>(&mut self, scope: F) -> (usize, R)
    where
        F: Fn(&mut Self) -> R,
    {
        let start = self.offset;
        let ret = scope(self);
        let delta = self.offset - start;
        (delta, ret)
    }

    /// Creates a scope in which exactly `count` bytes must be advanced (by
    /// reads or writes). This method shall be used as a sanity check scope.
    ///
    /// # Panics
    ///
    /// Panics if the scope didn't wrote `count` bytes. Notice that the panic
    /// message shall not be considered stable.
    pub fn scoped_exact<F, R>(&mut self, count: usize, scope: F) -> R
    where
        F: Fn(&mut Self) -> R,
    {
        let (delta, ret) = self.delta(scope);
        assert_eq!(delta, count);
        ret
    }
}

// Private utilities.
impl Buff<'_> {
    /// Attempts to get the slice with length `count` starting at the current
    /// length. Asserts that the current buffer has enough capacity to fit
    /// `count` more bytes.
    ///
    /// This method also increments `self.len` by `count`.
    #[inline(always)]
    fn slice_to(&mut self, count: usize) -> &mut [u8] {
        let lo = self.offset;
        let hi = lo + count;
        if hi > self.capacity() {
            panic!("not enough capacity for {count} more bytes");
        }
        self.offset = hi;
        &mut self.inner[lo..hi]
    }
}

impl fmt::Debug for Buff<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buff")
            .field("len", &self.offset)
            .field("remaining", &self.remaining())
            .field("capacity", &self.capacity())
            .field("inner", &"<bytes>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write() {
        let mut orig_buf = [0_u8; 10];

        let mut buf = Buff::new(&mut orig_buf);
        assert_eq!(buf.offset(), 0);

        buf.write(0x01ABCDEF_i32);
        assert_eq!(buf.offset(), 4);
        assert_eq!(buf.remaining(), 6);
        assert_eq!(buf.get(), b"\x01\xAB\xCD\xEF\x00\x00\x00\x00\x00\x00");

        buf.write(0x39C_u16);
        assert_eq!(buf.offset(), 6);
        assert_eq!(buf.remaining(), 4);
        assert_eq!(buf.get(), b"\x01\xAB\xCD\xEF\x03\x9C\x00\x00\x00\x00");

        buf.write_bytes(2, 3);
        assert_eq!(buf.offset(), 8);
        assert_eq!(buf.remaining(), 2);
        assert_eq!(buf.get(), b"\x01\xAB\xCD\xEF\x03\x9C\x03\x03\x00\x00");

        let data = [1, 2];
        buf.write_slice(&data);
        assert_eq!(buf.offset(), 10);
        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.get(), b"\x01\xAB\xCD\xEF\x03\x9C\x03\x03\x01\x02");
    }

    #[test]
    fn test_read() {
        let mut orig_buf = *b"\x01\xAB\xCD\xEF\x03\x9C\x03\x03\x01\x02";
        let mut buf = Buff::new(&mut orig_buf);

        let val: i32 = buf.read();
        assert_eq!(val, 0x01ABCDEF_i32);
        let val: u16 = buf.read();
        assert_eq!(val, 0x39C_u16);

        let mut dest = [0_u8; 4];
        buf.read_slice(&mut dest);
        assert_eq!(&dest, b"\x03\x03\x01\x02");
    }

    #[test]
    #[should_panic(expected = "not enough capacity for 4 more bytes")]
    fn test_overflow_write() {
        let mut orig_buf = [0; 4];
        let mut buf = Buff::new(&mut orig_buf);

        buf.write(16_i16);
        buf.write(32_i32); // BAM!
    }

    #[test]
    #[should_panic(expected = "not enough capacity for 1 more bytes")]
    fn test_overflow_read() {
        let mut orig_buf = [1, 2, 3, 4];
        let mut buf = Buff::new(&mut orig_buf);

        let _: i32 = buf.read();
        let _: u8 = buf.read(); // BAM!
    }

    #[test]
    fn test_seek() {
        let mut orig_buf = [1, 2, 3, 4];
        let mut buf = Buff::new(&mut orig_buf);

        let a: i32 = buf.read();
        buf.seek(0);
        let b: i32 = buf.read();
        assert_eq!(a, b);
    }

    #[test]
    fn test_delta() {
        let mut orig_buf = [1, 2, 3, 4];
        let mut buf = Buff::new(&mut orig_buf);

        let (delta, _) = buf.delta(|buf| {
            let _: u8 = buf.read();
            let _: u16 = buf.read();
        });
        assert_eq!(delta, 3);
        assert_eq!(buf.remaining(), 1);
    }

    #[test]
    fn scoped_exact_ok() {
        let mut orig_buf = [1, 2, 3, 4];
        let mut buf = Buff::new(&mut orig_buf);

        buf.scoped_exact(2, |buf| {
            let _: i16 = buf.read();
        });
    }

    #[test]
    #[should_panic]
    fn scoped_exact_panic() {
        let mut orig_buf = [1, 2, 3, 4];
        let mut buf = Buff::new(&mut orig_buf);

        buf.scoped_exact(2, |buf| {
            let _: i8 = buf.read();
        });
    }
}
