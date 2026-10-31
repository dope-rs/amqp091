use o3::base::cell::{local, region};

use crate::adapter::channel::Lease;
use crate::adapter::{conn, core};
use crate::failure::terminal;
use crate::service;
use crate::state::{channel, channels};

pub(in crate::adapter) struct View<'a, 'r, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    pub(in crate::adapter) index: channels::Index<'c, ID>,
    pub(in crate::adapter) channel: &'a mut channel::Session<'r, 'c, 'b, 'd, ID>,
}

/// Post-send authority that resolves a committed channel result before EOF retirement.
pub(in crate::adapter) struct Receipt<
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'b,
    const ID: u8,
    K: core::Capability,
> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, K>,
    epoch: conn::Epoch<'d, ID>,
    lease: Lease<'c, 'd, ID>,
}

impl<const ID: u8, K: core::Capability> Copy for Receipt<'_, '_, '_, '_, '_, ID, K> {}

impl<const ID: u8, K: core::Capability> Clone for Receipt<'_, '_, '_, '_, '_, ID, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K: core::Capability>
    Receipt<'o, 'r, 'c, 'b, 'd, ID, K>
{
    pub(in crate::adapter) const fn new(
        core: core::Core<'o, 'r, 'c, 'b, 'd, ID, K>,
        epoch: conn::Epoch<'d, ID>,
        lease: Lease<'c, 'd, ID>,
    ) -> Self {
        Self { core, epoch, lease }
    }

    pub(in crate::adapter) const fn core(self) -> core::Core<'o, 'r, 'c, 'b, 'd, ID, K> {
        self.core
    }

    pub(in crate::adapter) const fn lease(self) -> Lease<'c, 'd, ID> {
        self.lease
    }

    pub(in crate::adapter) fn collect(self, access: local::Access<'_, region::Identity<'d>>)
    where
        K: core::Allows<core::State>,
    {
        self.core.shared().write(access, |state| {
            state.routes.collect(self.lease.raw());
        });
    }

    /// Projects the retained result first; a missing result consults terminal epoch state.
    pub(in crate::adapter) fn with<R>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        f: impl for<'a> FnOnce(View<'a, 'r, 'c, 'b, 'd, ID>) -> Option<R>,
    ) -> service::EpochAccess<'c, 'b, 'd, R>
    where
        K: core::Allows<core::State>,
    {
        self.core.shared().write(access, |state| {
            if let Some(output) = state
                .routes
                .project(self.lease.raw(), |route| {
                    f(View {
                        index: route.index,
                        channel: route.channel,
                    })
                })
                .flatten()
            {
                return service::EpochAccess::Live(output);
            }
            state
                .connection
                .lifecycle
                .status()
                .terminal(self.epoch.raw())
                .map(terminal::Outcome::retain)
                .map_or(
                    service::EpochAccess::Unknown,
                    service::EpochAccess::Terminal,
                )
        })
    }
}

const _: () = assert!(
    std::mem::size_of::<Receipt<'static, 'static, 'static, 'static, 'static, 0, core::Publish>>()
        == std::mem::size_of::<
            core::Core<'static, 'static, 'static, 'static, 'static, 0, core::Publish>,
        >() + std::mem::size_of::<conn::Epoch<'static, 0>>()
            + std::mem::size_of::<Lease<'static, 'static, 0>>()
);
