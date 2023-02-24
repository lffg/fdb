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
    len: usize,
}

impl<'a> Buff<'a> {
    /// Creates a new fixed-size buffer, `Buff`.
    pub fn new(inner: &'a mut [u8]) -> Buff<'a> {
        Buff { inner, len: 0 }
    }

    /// Returns the buffer capacity.
    pub fn capacity(&self) -> usize {
        self.inner.len()
    }

    /// Returns the remaining available bytes in the buffer.
    pub fn remaining(&self) -> usize {
        self.capacity() - self.len()
    }

    /// Returns the buffer length.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns a view over the written portion of the inner slice.
    pub fn get(&self) -> &[u8] {
        &self.inner[..self.len]
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

    /// Creates a scope in which exactly `count` bytes must be advanced (by
    /// reads or writes). This method shall be used as a sanity check scope.
    ///
    /// # Panics
    ///
    /// Panics if the scope didn't wrote `count` bytes.
    pub fn scoped_exact<F>(&mut self, count: usize, scope: F)
    where
        F: Fn(&mut Self),
    {
        let start = self.len;
        scope(self);
        assert_eq!(self.len - start, count);
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
        let lo = self.len;
        let hi = lo + count;
        if hi > self.capacity() {
            panic!("not enough capacity for {count} more bytes");
        }
        self.len = hi;
        &mut self.inner[lo..hi]
    }
}

impl fmt::Debug for Buff<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buff")
            .field("len", &self.len())
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
        let mut orig_buf = [0_u8; 8];
        let mut buf = Buff::new(&mut orig_buf);

        assert_eq!(buf.len(), 0);
        buf.write(123_u32);
        assert_eq!(buf.len(), 4);
        assert_eq!(buf.get(), b"");
        buf.write(456_i32);
        assert_eq!(buf.len(), 8);
    }
}
