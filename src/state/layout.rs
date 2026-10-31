use std::mem;

/// Compile-time byte and alignment ceiling for internal state.
pub(crate) struct Layout {
    bytes: usize,
    alignment: usize,
}

impl Layout {
    pub(crate) const fn u64_words(words: usize) -> Self {
        let pointer = mem::align_of::<usize>();
        let wide = mem::align_of::<u64>();
        Self {
            bytes: words * mem::size_of::<u64>(),
            alignment: if pointer > wide { pointer } else { wide },
        }
    }

    pub(crate) const fn prove<T>(self) {
        assert!(mem::size_of::<T>() <= self.bytes);
        assert!(mem::align_of::<T>() <= self.alignment);
    }
}
