use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use dope_amqp091::adapter::{self, channel, conn, consumer, method};
use dope_amqp091::failure;
use dope_amqp091::protocol::method::Op;
use o3::base::cell::{local, region};

use crate::runtime::{self, ready};

pub(crate) struct Bind<'c, 'd, const ID: u8, M> {
    lease: channel::Lease<'c, 'd, ID>,
    method: M,
}

pub(crate) struct Run<
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M,
> {
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    ready: conn::Ready<'d, ID>,
    lease: channel::Lease<'c, 'd, ID>,
    call: method::Call<'o, 'r, 'c, 'b, 'd, ID, M>,
}

impl<'c, 'd, const ID: u8, M> Bind<'c, 'd, ID, M> {
    pub(crate) const fn new(lease: channel::Lease<'c, 'd, ID>, method: M) -> Self {
        Self { lease, method }
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, M>
    ready::Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID> for Bind<'c, 'd, ID, M>
where
    M: Op<'d>,
{
    type Run = Run<'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>;

    fn start(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        access: local::Access<'_, region::Identity<'d>>,
        ready: conn::Ready<'d, ID>,
    ) -> Result<Self::Run, failure::Runtime<'c, 'b, 'd, ID>> {
        let call = handle
            .adapter
            .methods()
            .reserve(access, ready, self.lease, self.method)?;
        Ok(Run {
            handle,
            ready,
            lease: self.lease,
            call,
        })
    }
}

impl<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8, M: Op<'d>>
    adapter::Poll<'c, 'b, 'd, ID> for Run<'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>
{
    type Output = (
        runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        conn::Ready<'d, ID>,
        channel::Lease<'c, 'd, ID>,
        M::Output<'b, consumer::Key<'o, 'd, ID>>,
    );

    fn poll(
        &mut self,
        access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<Self::Output, failure::Runtime<'c, 'b, 'd, ID>>> {
        self.call
            .poll(access, wake)
            .map_ok(|output| (self.handle, self.ready, self.lease, output))
    }
}

const _: () = assert!(
    mem::size_of::<Bind<'static, 'static, 0, dope_amqp091::protocol::method::channel::Open>>()
        == mem::size_of::<channel::Lease<'static, 'static, 0>>()
);
