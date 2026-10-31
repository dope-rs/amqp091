use std::mem;

use crate::failure::{self, terminal};
use crate::protocol::wire::text::resident;

/// One-word, lifetime- and route-branded terminal cause.
#[derive(Debug)]
#[repr(transparent)]
pub struct Cause<'c, 'b, 'd, const ID: u8> {
    raw: terminal::Lease<'c, 'b, 'd>,
}

impl<'c, 'b, 'd, const ID: u8> Cause<'c, 'b, 'd, ID> {
    pub(crate) const fn from_raw(raw: terminal::Lease<'c, 'b, 'd>) -> Self {
        Self { raw }
    }

    pub fn generation(&self) -> u64 {
        self.raw.generation()
    }

    pub fn error(&self) -> &failure::Error<resident::Short<'b, 'd>> {
        self.raw.error()
    }

    pub fn is_same(&self, other: &Self) -> bool {
        self.raw.is_same(&other.raw)
    }
}

const _: () =
    assert!(mem::size_of::<Cause<'static, 'static, 'static, 0>>() == mem::size_of::<usize>());
const _: () = assert!(
    mem::size_of::<Option<Cause<'static, 'static, 'static, 0>>>() == mem::size_of::<usize>()
);
