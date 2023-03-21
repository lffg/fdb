use crate::util::io::Size;

/// A buffer that holds [`Size`] objects, up to a maximum size.
pub struct SizeBuf<T> {
    inner: Vec<T>,
    max: usize,
    used: usize,
}

impl<T> SizeBuf<T> {
    /// Constructs a new [`SizeBuf`].
    pub fn new(max_size: usize) -> Self {
        SizeBuf {
            inner: Vec::new(),
            max: max_size,
            used: 0,
        }
    }

    /// Empties the underlying buffer.
    pub fn clear(&mut self) {
        self.inner.clear()
    }

    /// Checks if the buffer has capacity for more `n` bytes.
    pub fn can_accommodate(&self, n: usize) -> bool {
        self.used + n <= self.max
    }

    /// Attempts to insert the given element in the buffer. Fails otherwise.
    pub fn try_push(&mut self, value: T) -> Result<(), T>
    where
        T: Size,
    {
        if self.can_accommodate(value.size() as usize) {
            self.inner.push(value);
            Ok(())
        } else {
            Err(value)
        }
    }

    /// Returns a mutable reference to the underlying buffer.
    pub fn as_inner_mut(&mut self) -> &mut Vec<T> {
        &mut self.inner
    }

    /// Returns the underlying buffer.
    pub fn into_inner(self) -> Vec<T> {
        self.inner
    }
}

impl<T> IntoIterator for SizeBuf<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}
