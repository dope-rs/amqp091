use std::mem;

use o3::depot::arena::pool::Shared;
use o3::mem::rc;

use crate::failure::{self, terminal};
use crate::protocol::wire::text::resident;

/// Internal, route-erased retained reference used by channel state.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct Lease<'c, 'b, 'd>(Shared<'c, terminal::Payload<'b, 'd>>);

impl<'c, 'b, 'd> Lease<'c, 'b, 'd> {
    pub(super) const fn new(lease: Shared<'c, terminal::Payload<'b, 'd>>) -> Self {
        Self(lease)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.0.generation
    }

    pub(crate) fn error(&self) -> &failure::Error<resident::Short<'b, 'd>> {
        &self.0.error
    }

    pub(crate) fn is_same(&self, other: &Self) -> bool {
        self.0.is_same(&other.0)
    }
}

impl Lease<'_, '_, '_> {
    pub(crate) fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self(self.0.try_clone()?))
    }
}

const _: () =
    assert!(mem::size_of::<Lease<'static, 'static, 'static>>() == mem::size_of::<usize>());
