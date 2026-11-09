use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use dope_amqp091::adapter::{self, channel, conn};
use dope_amqp091::failure;
use o3::base::cell::{local, region};

use crate::channel::Channel;
use crate::runtime::{self, ready};

pub(crate) struct Open {
    id: u16,
}

impl Open {
    pub(crate) const fn new(id: u16) -> Self {
        Self { id }
    }
}

pub(crate) struct Bound {
    id: u16,
}

impl Bound {
    pub(crate) const fn new(id: u16) -> Self {
        Self { id }
    }
}

pub(crate) struct Auto;

pub(crate) struct Run<
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    A,
> {
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    access: A,
    open: channel::Open<'o, 'r, 'c, 'b, 'd, ID>,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    ready::Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID> for Open
{
    type Run = Run<'app, 'o, 'r, 'c, 'b, 'd, B, ID, ()>;

    fn start(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        mut access: local::Access<'_, region::Identity<'d>>,
        ready: conn::Ready<'d, ID>,
    ) -> Result<Self::Run, failure::Runtime<'c, 'b, 'd, ID>> {
        let amqp = handle.adapter;
        let slot = amqp.channels().reserve(access.reborrow(), ready, self.id)?;
        let open = slot.open(amqp, access, ready)?;
        Ok(Run {
            handle,
            access: (),
            open,
        })
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    ready::Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID> for Bound
{
    type Run = Run<'app, 'o, 'r, 'c, 'b, 'd, B, ID, conn::Ready<'d, ID>>;

    fn start(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        mut access: local::Access<'_, region::Identity<'d>>,
        ready: conn::Ready<'d, ID>,
    ) -> Result<Self::Run, failure::Runtime<'c, 'b, 'd, ID>> {
        let amqp = handle.adapter;
        let slot = amqp.channels().reserve(access.reborrow(), ready, self.id)?;
        let open = slot.open(amqp, access, ready)?;
        Ok(Run {
            handle,
            access: ready,
            open,
        })
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    ready::Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID> for Auto
{
    type Run = Run<'app, 'o, 'r, 'c, 'b, 'd, B, ID, ()>;

    fn start(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        mut access: local::Access<'_, region::Identity<'d>>,
        ready: conn::Ready<'d, ID>,
    ) -> Result<Self::Run, failure::Runtime<'c, 'b, 'd, ID>> {
        let amqp = handle.adapter;
        let slot = amqp.channels().next(access.reborrow(), ready)?;
        let open = slot.open(amqp, access, ready)?;
        Ok(Run {
            handle,
            access: (),
            open,
        })
    }
}

impl<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8, A: Copy>
    adapter::Poll<'c, 'b, 'd, ID> for Run<'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>
{
    type Output = Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID, A>;

    fn poll(
        &mut self,
        access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<Self::Output, failure::Runtime<'c, 'b, 'd, ID>>> {
        self.open.poll(access, wake).map_ok(|lease| Channel {
            handle: self.handle,
            lease,
            access: self.access,
        })
    }
}

const _: () = {
    assert!(mem::size_of::<Open>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Bound>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Auto>() == 0);
};
