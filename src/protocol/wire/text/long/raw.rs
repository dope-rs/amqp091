use std::num::NonZero;

use o3::alloc::{self, VecExt as _};

use super::{Buf, Long};
use crate::failure::{self, wire};

impl Buf {
    pub(crate) fn try_plain(user: &str, password: &str) -> Result<Self, failure::Error> {
        let parts: [&[u8]; 4] = [b"\0", user.as_bytes(), b"\0", password.as_bytes()];
        let target = wire::Allocation::Authentication;
        let len = parts
            .iter()
            .try_fold(0usize, |len, part| len.checked_add(part.len()))
            .ok_or_else(|| failure::Error::Allocation {
                target,
                error: alloc::Error::Overflow,
            })?;
        Long::check_len(len)?;
        for (base, value) in [(1, user), (user.len() + 2, password)] {
            if let Some(offset) = value.find('\0') {
                // Length admission bounds this positive wire offset below len.
                return Err(failure::Error::InvalidConfig(
                    wire::Config::AuthenticationNul {
                        offset: NonZero::<usize>::MIN.saturating_add(base - 1 + offset),
                    },
                ));
            }
        }
        let mut value =
            Vec::try_new(len).map_err(|error| failure::Error::Allocation { target, error })?;
        // SAFETY: the retained immutable parts sum to len. The fresh Vec
        // admits that disjoint, exclusive, isize-bounded range; copying
        // initializes it completely before publishing its length.
        unsafe {
            o3::buffer::raw::copy_slices(parts.iter().copied(), value.as_mut_ptr());
            value.set_len(len);
        }
        Ok(Self(value))
    }
}
