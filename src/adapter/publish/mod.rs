use std::mem;

use o3::base::cell::{local, region};

use crate::adapter::channel::{self, route};
use crate::adapter::{conn, core};
use crate::failure;
use crate::protocol::wire::frame;

mod confirm;
mod limit;
mod request;
mod send;

pub use confirm::Confirm;
pub use request::Request;
pub use send::Send;

pub(super) const FRAME_OVERHEAD: usize = frame::OVERHEAD;
pub(super) const METHOD_FIXED_SIZE: usize = frame::OVERHEAD + 9;

#[repr(transparent)]
pub struct Publish<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
}

impl<const ID: u8> Copy for Publish<'_, '_, '_, '_, '_, ID> {}

impl<const ID: u8> Clone for Publish<'_, '_, '_, '_, '_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Publish<'o, 'r, 'c, 'b, 'd, ID> {
    pub(super) const fn new(core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Publish>) -> Self {
        Self { core }
    }

    pub fn send<'exchange, 'key, 'properties, 'body>(
        self,
        access: local::Read<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        draft: Request<'exchange, 'key, 'properties, 'body>,
    ) -> Result<
        Send<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>,
        failure::Runtime<'c, 'b, 'd, ID>,
    > {
        let (route, input) = self.prepare(access, target, lease, draft)?;
        Ok(Send::new(route, input))
    }

    pub fn confirm<'exchange, 'key, 'properties, 'body>(
        self,
        access: local::Read<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        draft: Request<'exchange, 'key, 'properties, 'body>,
    ) -> Result<
        Confirm<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>,
        failure::Runtime<'c, 'b, 'd, ID>,
    > {
        let (route, input) = self.prepare(access, target, lease, draft)?;
        Ok(Confirm::new(route, input))
    }

    fn prepare<'exchange, 'key, 'properties, 'body>(
        self,
        access: local::Read<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        draft: Request<'exchange, 'key, 'properties, 'body>,
    ) -> Result<
        (
            route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
            request::Checked<'exchange, 'key, 'properties, 'body>,
        ),
        failure::Runtime<'c, 'b, 'd, ID>,
    > {
        let route = route::Route::new(self.core, target, lease);
        let limit = limit::Limit::read(route, access)?;
        let input = match draft.check(limit) {
            Ok(input) => input,
            Err(error) => return Err(failure::Runtime::from(error)),
        };
        Ok((route, input))
    }
}

const _: () = assert!(
    mem::size_of::<Publish<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<usize>()
);
