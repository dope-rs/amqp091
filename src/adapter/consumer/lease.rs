use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::fanout;
use o3::base::cell::{local, region};
use o3::mem::slot;

use crate::adapter::{channel, conn, consumer};
use crate::message::ack;
use crate::{failure, protocol};

/// One logical consumer and its current exact transport source.
pub struct Lease<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M: consumer::Mode = ack::Manual>
{
    pub(super) root: consumer::Consumers<'o, 'r, 'c, 'b, 'd, ID>,
    pub(super) channel: channel::Lease<'c, 'd, ID>,
    pub(super) key: slot::Storage<protocol::ConsumerKey<consumer::Key<'o, 'd, ID>, M>>,
    pub(super) ready: Option<conn::Ready<'d, ID>>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M: consumer::Mode>
    Lease<'o, 'r, 'c, 'b, 'd, ID, M>
{
    pub(super) const fn new(
        root: consumer::Consumers<'o, 'r, 'c, 'b, 'd, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        key: protocol::ConsumerKey<consumer::Key<'o, 'd, ID>, M>,
        ready: conn::Ready<'d, ID>,
    ) -> Self {
        Self {
            root,
            channel: lease,
            key: slot::Storage::filled(key, ()),
            ready: Some(ready),
        }
    }

    pub fn channel_id(&self) -> u16 {
        self.channel.id()
    }

    pub fn recv(&mut self) -> consumer::Recv<'_, 'o, 'r, 'c, 'b, 'd, ID, M> {
        consumer::Recv::new(self)
    }

    pub fn poll_state(
        &self,
        mut access: local::Access<'_, region::Identity<'d>>,
        wait: &mut fanout::Registration<'o, 'd>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<Option<consumer::Snapshot<'b, 'd>>, failure::Runtime<'c, 'b, 'd, ID>>>
    {
        let Some(key) = self.key.get() else {
            return task::Poll::Ready(Ok(None));
        };
        let watch = conn::Observe {
            core: self.root.core.project(),
        };
        let ready = task::ready!(watch.poll_ready(access.reborrow(), wait, wake))?;
        task::Poll::Ready(self.root.snapshot(access, ready, self.channel, key))
    }
}

const _: () = {
    assert!(
        mem::size_of::<Option<conn::Ready<'static, 0>>>()
            == mem::size_of::<conn::Ready<'static, 0>>()
    );
    assert!(
        mem::size_of::<Lease<'static, 'static, 'static, 'static, 'static, 0>>()
            == 8 * mem::size_of::<usize>()
    );
};
