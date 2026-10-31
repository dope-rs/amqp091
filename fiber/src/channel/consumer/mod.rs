use std::task;

use dope::core::driver::schedule::ready::wake::fanout;
use dope::runtime::client;
use dope_amqp091::adapter::{channel, conn, consumer};
use dope_amqp091::failure;
use dope_amqp091::message::ack;
use dope_amqp091::protocol::wire::text::resident;
use dope_amqp091::protocol::{self};
use dope_fiber::abi;
use o3::base::cell::local;

use crate::runtime;

mod delivery;
mod recv;
mod settle;

pub use delivery::Delivery;
pub use recv::Recv;
pub use settle::Settle;

pub struct Consumer<
    'app,
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M: consumer::Mode = ack::Manual,
> {
    lease: consumer::Lease<'o, 'r, 'c, 'b, 'd, ID, M>,
    tag: resident::Short<'b, 'd>,
    bound: client::Bound<'app, 'd, B>,
}

impl<
    'o: 'app,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'app,
    'd: 'app + 'b,
    B: ?Sized,
    const ID: u8,
    M: consumer::Mode,
> Consumer<'app, 'o, 'r, 'c, 'b, 'd, B, ID, M>
{
    pub(crate) fn try_new(
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        ready: conn::Ready<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        key: protocol::ConsumerKey<consumer::Key<'o, 'd, ID>, M>,
        tag: resident::Short<'b, 'd>,
    ) -> Result<Self, failure::Runtime<'c, 'b, 'd, ID>> {
        Ok(Self {
            lease: handle.adapter.consumers().try_bind(ready, lease, key)?,
            tag,
            bound: handle.bound(),
        })
    }

    pub fn channel_id(&self) -> u16 {
        self.lease.channel_id()
    }

    pub fn tag(&self) -> &str {
        self.tag.as_ref()
    }

    pub fn recv(&mut self) -> Recv<'_, 'app, 'o, 'r, 'c, 'b, 'd, B, ID, M> {
        Recv::new(self.lease.recv(), self.bound)
    }

    pub fn snapshot(
        &self,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<consumer::Snapshot<'b, 'd>, failure::Runtime<'c, 'b, 'd, ID>>,
    > + '_ {
        let lease = &self.lease;
        let mut wait = fanout::Registration::new();
        abi::poll_fn(move |mut cx| {
            let wake = cx.as_ref().completion();
            let local = local::Access::new(cx.as_mut().region_token());
            match lease.poll_state(local, &mut wait, wake) {
                task::Poll::Pending => task::Poll::Pending,
                task::Poll::Ready(Ok(Some(state))) => task::Poll::Ready(Ok(state)),
                task::Poll::Ready(Ok(None)) => {
                    task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Closed)))
                }
                task::Poll::Ready(Err(error)) => task::Poll::Ready(Err(error)),
            }
        })
    }
}

const _: () = {
    assert!(
        size_of::<Consumer<'static, 'static, 'static, 'static, 'static, 'static, (), 0, ack::Manual>>(
        ) == size_of::<
            Consumer<'static, 'static, 'static, 'static, 'static, 'static, (), 0, ack::Auto>,
        >()
    );
    assert!(
        size_of::<Consumer<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
            == size_of::<consumer::Lease<'static, 'static, 'static, 'static, 'static, 0>>()
                + size_of::<resident::Short<'static, 'static>>()
    );
};
