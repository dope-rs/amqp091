use o3::base::cell::{local, region};

use crate::adapter::channel::{Lease, receipt};
use crate::adapter::{conn, core};
use crate::protocol::wire::Writer;
use crate::state::shared;
use crate::{failure, service};

mod view;
pub(in crate::adapter) use view::View;

pub struct Route<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K: core::Capability> {
    pub(in crate::adapter) gate: core::Gate<'o, 'r, 'c, 'b, 'd, ID, K>,
    pub(in crate::adapter) lease: Lease<'c, 'd, ID>,
}

impl<const ID: u8, K: core::Capability> Copy for Route<'_, '_, '_, '_, '_, ID, K> {}

impl<const ID: u8, K: core::Capability> Clone for Route<'_, '_, '_, '_, '_, ID, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K: core::Capability>
    Route<'o, 'r, 'c, 'b, 'd, ID, K>
{
    pub(in crate::adapter) const fn new(
        core: core::Core<'o, 'r, 'c, 'b, 'd, ID, K>,
        target: conn::Ready<'d, ID>,
        lease: Lease<'c, 'd, ID>,
    ) -> Self {
        Self {
            gate: core::Gate::new(core, target),
            lease,
        }
    }

    pub(in crate::adapter) const fn core(self) -> core::Core<'o, 'r, 'c, 'b, 'd, ID, K> {
        self.gate.core()
    }

    pub(in crate::adapter) const fn epoch(self) -> conn::Epoch<'d, ID> {
        self.gate.epoch()
    }

    pub(in crate::adapter) const fn lease(self) -> Lease<'c, 'd, ID> {
        self.lease
    }

    /// Finishes active routing and retains only result-resolution authority.
    pub(in crate::adapter) const fn receipt(self) -> receipt::Receipt<'o, 'r, 'c, 'b, 'd, ID, K> {
        receipt::Receipt::new(self.core(), self.epoch(), self.lease)
    }

    pub(in crate::adapter) fn read<R>(
        self,
        access: local::Read<'_, region::Identity<'d>>,
        f: impl for<'s> FnOnce(&'s shared::Session<'r, 'c, 'b, 'd, ID>) -> R,
    ) -> service::EpochAccess<'c, 'b, 'd, R>
    where
        K: core::Allows<core::State>,
    {
        self.gate.read(access, f)
    }

    pub(in crate::adapter) fn with<R>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        f: impl for<'conn, 'a, 's, 'v> FnOnce(View<'conn, 'a, 's, 'v, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> service::EpochAccess<'c, 'b, 'd, Option<R>>
    where
        K: core::Allows<core::State>,
    {
        self.gate.with(access, |state| {
            let conn = &state.connection;
            state.routes.project(self.lease.raw(), |route| {
                f(View::new(conn, route.index, route.channel, route.topology))
            })
        })
    }

    pub(in crate::adapter) fn admit<R>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        encode: impl FnOnce(&mut Writer),
        commit: impl for<'conn, 'a, 's, 'v> FnOnce(
            View<'conn, 'a, 's, 'v, 'r, 'c, 'b, 'd, ID>,
        ) -> Result<R, failure::Error>,
    ) -> core::Admit<'c, 'b, 'd, R, ID>
    where
        K: core::Allows<core::State> + core::Allows<core::Egress>,
    {
        self.gate
            .admit(access, Some(self.lease.raw()), encode, |state| {
                let conn = &state.connection;
                let Some(output) = state.routes.project(self.lease.raw(), |route| {
                    commit(View::new(conn, route.index, route.channel, route.topology))
                }) else {
                    return Err(failure::Error::Closed);
                };
                output
            })
    }

    pub(in crate::adapter) fn try_frame<P, R, D>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        encode: impl FnOnce(&mut Writer) -> P,
        commit: impl for<'conn, 'a, 's, 'v> FnOnce(
            View<'conn, 'a, 's, 'v, 'r, 'c, 'b, 'd, ID>,
            &[u8],
            P,
        ) -> Result<R, core::Reject<'b, 'd, D>>,
    ) -> Result<core::Admit<'c, 'b, 'd, R, ID>, D>
    where
        K: core::Allows<core::State> + core::Allows<core::Egress>,
    {
        self.gate.try_frame(
            access,
            Some(self.lease.raw()),
            encode,
            |state, frame, proof| {
                let conn = &state.connection;
                let Some(output) = state.routes.project(self.lease.raw(), |route| {
                    commit(
                        View::new(conn, route.index, route.channel, route.topology),
                        frame,
                        proof,
                    )
                }) else {
                    return Err(core::Reject::Error(failure::Error::Closed));
                };
                output
            },
        )
    }
}

const _: () = assert!(
    std::mem::size_of::<Route<'static, 'static, 'static, 'static, 'static, 0, core::Publish>>()
        == 4 * std::mem::size_of::<usize>()
);
