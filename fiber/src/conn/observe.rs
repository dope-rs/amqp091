use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::adapter::conn;
use dope_amqp091::failure;
use dope_fiber::abi;
use o3::base::cell::local;

use crate::conn::{Lease, Retired};
use crate::runtime;

/// Read-only connection facet.
#[repr(transparent)]
pub struct Observe<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8> {
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
}

impl<B: ?Sized, const ID: u8> Copy for Observe<'_, '_, '_, '_, '_, '_, B, ID> {}

impl<B: ?Sized, const ID: u8> Clone for Observe<'_, '_, '_, '_, '_, '_, B, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Observe<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(super) const fn new(handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>) -> Self {
        Self { handle }
    }

    pub fn epoch(
        self,
    ) -> impl abi::Fiber<'d, Output = Result<conn::Epoch<'d, ID>, failure::Runtime<'c, 'b, 'd, ID>>>
    {
        let handle = self.handle;
        let mut wait: fanout::Registration<'o, 'd> = fanout::Registration::new();
        abi::poll_fn(move |mut cx| {
            let wake = cx.as_ref().completion();
            let local = local::Access::new(cx.as_mut().region_token());
            handle
                .adapter
                .connection()
                .observe()
                .poll_epoch(local, &mut wait, wake)
        })
    }

    pub fn ready(
        self,
    ) -> impl abi::Fiber<'d, Output = Result<conn::Ready<'d, ID>, failure::Runtime<'c, 'b, 'd, ID>>>
    {
        let mut ready = runtime::ready::Auto::new(self.handle);
        abi::poll_fn(move |cx| runtime::ready::Source::poll(&mut ready, cx))
    }

    /// Executes any number of state observations in one domain access.
    pub fn read<R>(
        self,
        f: impl for<'state> FnOnce(conn::View<'state, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> impl abi::Fiber<'d, Output = R> {
        use dope_fiber::task::local::Once;

        let handle = self.handle;
        Once::new(move |local| handle.adapter.connection().observe().read(local.read(), f))
    }

    /// Samples the exact topology-ready transport lifetime.
    pub fn lease(
        self,
        ready: conn::Ready<'d, ID>,
    ) -> impl abi::Fiber<'d, Output = Result<Option<Lease<'d, ID>>, failure::Runtime<'c, 'b, 'd, ID>>>
    {
        use dope_fiber::task::local::Once;

        let handle = self.handle;
        Once::new(move |local| {
            handle
                .adapter
                .connection()
                .observe()
                .read_in(local.read(), ready, |view| view.deadline())
                .map(|lease| {
                    lease.map(|(cycle, until)| Lease {
                        ready,
                        generation: cycle,
                        expires_at: until,
                    })
                })
        })
    }

    pub fn unblocked(
        self,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> {
        let handle = self.handle;
        let mut wait = fanout::Registration::new();
        abi::poll_fn(move |mut cx| {
            let wake = cx.as_ref().completion();
            let local = local::Access::new(cx.as_mut().region_token());
            handle
                .adapter
                .connection()
                .observe()
                .poll_unblock(local, &mut wait, wake)
        })
    }

    /// Waits until the exact transport generation is no longer active.
    pub fn retired(self, epoch: conn::Epoch<'d, ID>) -> Retired<'o, 'r, 'c, 'b, 'd, ID> {
        let state = self.handle.adapter.connection().observe();
        Retired::new(state, epoch)
    }
}

const _: () = assert!(
    std::mem::size_of::<Observe<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == std::mem::size_of::<usize>()
);
