use std::mem;

use dope::core::driver::route;
use dope::manifold::client::conn;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Id<'d, const ROUTE: u8>(conn::Id<'d, route::KeyTag<ROUTE>>);

impl<'d, const ROUTE: u8> Id<'d, ROUTE> {
    pub(crate) const fn new(token: conn::Id<'d, route::KeyTag<ROUTE>>) -> Self {
        Self(token)
    }

    pub(crate) const fn token(self) -> conn::Id<'d, route::KeyTag<ROUTE>> {
        self.0
    }
}

const _: () = assert!(
    mem::size_of::<Id<'static, 0>>() == mem::size_of::<conn::Id<'static, route::KeyTag<0>>>()
);
const _: () = assert!(
    mem::align_of::<Id<'static, 0>>() == mem::align_of::<conn::Id<'static, route::KeyTag<0>>>()
);
