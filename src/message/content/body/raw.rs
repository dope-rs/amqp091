use o3::alloc::{self, VecExt as _};

use super::Body;

impl Body<'_, '_, '_> {
    pub fn try_to_vec(&self) -> Result<Vec<u8>, alloc::Error> {
        let len = self.len();
        let mut body = Vec::try_new(len)?;
        // SAFETY: the completed immutable chunks cover exactly len bytes. The
        // fresh Vec admits that disjoint, exclusive, isize-bounded range;
        // copying initializes it completely before publishing its length.
        unsafe {
            o3::buffer::raw::copy_slices(self.chunks(), body.as_mut_ptr());
            body.set_len(len);
        }
        Ok(body)
    }
}

impl PartialEq<[u8]> for Body<'_, '_, '_> {
    fn eq(&self, mut other: &[u8]) -> bool {
        self.len() == other.len()
            && self.chunks().all(|chunk| {
                // SAFETY: the equal-length slice retains the suffix after
                // every preceding chunk of this complete immutable body.
                let (prefix, rest) = unsafe { other.split_at_unchecked(chunk.len()) };
                other = rest;
                chunk == prefix
            })
    }
}
