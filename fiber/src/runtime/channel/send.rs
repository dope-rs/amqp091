use std::mem;

use dope_amqp091::adapter::{channel, conn, publish};
use dope_amqp091::{failure, protocol};
use o3::base::cell::{local, region};

use crate::runtime::{self, ready};

pub(crate) struct Send<
    'exchange,
    'key,
    'properties,
    'body,
    'c,
    'd,
    const CONFIRM: bool,
    const ID: u8,
> {
    lease: channel::Lease<'c, 'd, ID>,
    req: publish::Request<'exchange, 'key, 'properties, 'body>,
}

impl<'exchange, 'key, 'properties, 'body, 'c, 'd, const CONFIRM: bool, const ID: u8>
    Send<'exchange, 'key, 'properties, 'body, 'c, 'd, CONFIRM, ID>
{
    pub(crate) const fn new(
        lease: channel::Lease<'c, 'd, ID>,
        req: publish::Request<'exchange, 'key, 'properties, 'body>,
    ) -> Self {
        Self { lease, req }
    }
}

impl<
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'exchange,
    'key,
    'properties,
    'body,
    'app,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
> ready::Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
    for Send<'exchange, 'key, 'properties, 'body, 'c, 'd, false, ID>
{
    type Output = ();
    type Run = publish::Send<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>;

    fn start(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        access: local::Access<'_, region::Identity<'d>>,
        ready: conn::Ready<'d, ID>,
    ) -> Result<Self::Run, failure::Runtime<'c, 'b, 'd, ID>> {
        handle
            .adapter
            .publish()
            .send(access.read(), ready, self.lease, self.req)
    }
}

impl<
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'exchange,
    'key,
    'properties,
    'body,
    'app,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
> ready::Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
    for Send<'exchange, 'key, 'properties, 'body, 'c, 'd, true, ID>
{
    type Output = protocol::Confirmation;
    type Run = publish::Confirm<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>;

    fn start(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        access: local::Access<'_, region::Identity<'d>>,
        ready: conn::Ready<'d, ID>,
    ) -> Result<Self::Run, failure::Runtime<'c, 'b, 'd, ID>> {
        handle
            .adapter
            .publish()
            .confirm(access.read(), ready, self.lease, self.req)
    }
}
const _: () = {
    assert!(
        mem::size_of::<
            ready::Op<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                (),
                0,
                ready::Exact<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
                Send<'static, 'static, 'static, 'static, 'static, 'static, false, 0>,
            >,
        >() == 15 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<
            ready::Op<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                (),
                0,
                ready::Auto<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
                Send<'static, 'static, 'static, 'static, 'static, 'static, false, 0>,
            >,
        >() == 15 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<
            ready::Op<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                (),
                0,
                ready::Exact<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
                Send<'static, 'static, 'static, 'static, 'static, 'static, true, 0>,
            >,
        >() == 19 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<
            ready::Op<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                (),
                0,
                ready::Auto<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
                Send<'static, 'static, 'static, 'static, 'static, 'static, true, 0>,
            >,
        >() == 19 * mem::size_of::<usize>()
    );
};
