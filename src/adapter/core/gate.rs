use o3::base::cell::{local, region};

use crate::adapter::{conn, core};
use crate::failure::terminal;
use crate::protocol::wire::Writer;
use crate::protocol::wire::text::resident;
use crate::state::conn::core::Core;
use crate::state::{channels, shared};
use crate::{failure, service};

pub(in crate::adapter) struct Gate<
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'b,
    const ID: u8,
    K: core::Capability,
> {
    pub(in crate::adapter) core: core::Core<'o, 'r, 'c, 'b, 'd, ID, K>,
    ready: conn::Ready<'d, ID>,
}

impl<const ID: u8, K: core::Capability> Copy for Gate<'_, '_, '_, '_, '_, ID, K> {}

impl<const ID: u8, K: core::Capability> Clone for Gate<'_, '_, '_, '_, '_, ID, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K: core::Capability>
    Gate<'o, 'r, 'c, 'b, 'd, ID, K>
{
    pub(in crate::adapter) const fn new(
        core: core::Core<'o, 'r, 'c, 'b, 'd, ID, K>,
        ready: conn::Ready<'d, ID>,
    ) -> Self {
        Self { core, ready }
    }

    pub(in crate::adapter) const fn core(self) -> core::Core<'o, 'r, 'c, 'b, 'd, ID, K> {
        self.core
    }

    pub(in crate::adapter) const fn epoch(self) -> conn::Epoch<'d, ID> {
        self.ready.epoch()
    }

    pub(in crate::adapter) fn read<R>(
        self,
        access: local::Read<'_, region::Identity<'d>>,
        f: impl for<'s> FnOnce(&'s shared::Session<'r, 'c, 'b, 'd, ID>) -> R,
    ) -> service::EpochAccess<'c, 'b, 'd, R>
    where
        K: core::Allows<core::State>,
    {
        let target = self.epoch().raw();
        let active = self.core.is_active(target);
        self.core.shared().read(access, |state| {
            if active && state.connection.lifecycle.status().ready() == Some(target) {
                service::EpochAccess::Live(f(state))
            } else {
                self.terminal(&state.connection)
            }
        })
    }

    pub(in crate::adapter) fn with<R>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        f: impl for<'s> FnOnce(&'s mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> service::EpochAccess<'c, 'b, 'd, R>
    where
        K: core::Allows<core::State>,
    {
        let target = self.epoch().raw();
        let active = self.core.is_active(target);
        self.core.shared().write(access, |state| {
            if active && state.connection.lifecycle.status().ready() == Some(target) {
                service::EpochAccess::Live(f(state))
            } else {
                self.terminal(state.connection)
            }
        })
    }

    pub(in crate::adapter) fn status(
        self,
        access: local::Access<'_, region::Identity<'d>>,
    ) -> service::EpochAccess<'c, 'b, 'd, ()>
    where
        K: core::Allows<core::State>,
    {
        self.with(access, |_| ())
    }

    pub(in crate::adapter) fn admit<R>(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        lease: Option<channels::Lease<'d, ID>>,
        encode: impl FnOnce(&mut Writer),
        commit: impl for<'s> FnOnce(
            &'s mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>,
        ) -> Result<R, failure::Error>,
    ) -> core::Admit<'c, 'b, 'd, R, ID>
    where
        K: core::Allows<core::State> + core::Allows<core::Egress>,
    {
        let target = self.epoch().raw();
        let result =
            self.core
                .outbound()
                .admit(access.reborrow(), target, lease, encode, |state| {
                    if state.connection.lifecycle.status().ready() == Some(target) {
                        commit(state)
                    } else {
                        Err(failure::Error::Closed)
                    }
                });
        self.finish(access, result)
    }

    pub(in crate::adapter) fn try_frame<P, R, D>(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        lease: Option<channels::Lease<'d, ID>>,
        encode: impl FnOnce(&mut Writer) -> P,
        commit: impl FnOnce(
            &mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>,
            &[u8],
            P,
        ) -> Result<R, core::Reject<'b, 'd, D>>,
    ) -> Result<core::Admit<'c, 'b, 'd, R, ID>, D>
    where
        K: core::Allows<core::State> + core::Allows<core::Egress>,
    {
        let target = self.epoch().raw();
        let result = self.core.outbound().try_frame(
            access.reborrow(),
            target,
            lease,
            encode,
            |region, frame, proof| {
                self.core
                    .shared()
                    .write(local::Access::new(region), |state| {
                        if state.connection.lifecycle.status().ready() != Some(target) {
                            return Err(core::Reject::Error(failure::Error::Closed));
                        }
                        state
                            .connection
                            .try_frame(frame.len())
                            .map_err(core::Reject::Error)?;
                        commit(state, frame, proof)
                    })
            },
        );
        match result {
            Ok(result) => Ok(self.finish(access, result)),
            Err(core::Reject::Error(error)) => Ok(core::Admit::Error(self.error(access, error))),
            Err(core::Reject::Defer(defer)) => Err(defer),
        }
    }

    fn finish<R>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        result: service::Admit<R>,
    ) -> core::Admit<'c, 'b, 'd, R, ID>
    where
        K: core::Allows<core::State>,
    {
        match result {
            service::Admit::Item(item) => core::Admit::Item(item),
            service::Admit::Unavailable => match self.status(access) {
                service::EpochAccess::Live(()) => core::Admit::Full,
                service::EpochAccess::Terminal(cause) => core::Admit::Error(cause.branded()),
                service::EpochAccess::Unknown => {
                    core::Admit::Error(failure::Runtime::from(failure::Error::Closed))
                }
            },
            service::Admit::Inactive => {
                core::Admit::Error(self.error(access, failure::Error::Closed))
            }
            service::Admit::Error(error) => core::Admit::Error(self.error(access, error.into())),
        }
    }

    fn error(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        error: failure::Error<resident::Short<'b, 'd>>,
    ) -> failure::Runtime<'c, 'b, 'd, ID>
    where
        K: core::Allows<core::State>,
    {
        self.core.shared().read(access.read(), |state| {
            state
                .connection
                .lifecycle
                .status()
                .terminal(self.epoch().raw())
                .map(terminal::Outcome::retain)
                .map_or(failure::Runtime::Local(error), terminal::Outcome::branded)
        })
    }

    fn terminal<R>(self, state: &Core<'c, 'b, 'd, ID>) -> service::EpochAccess<'c, 'b, 'd, R> {
        state
            .lifecycle
            .status()
            .terminal(self.epoch().raw())
            .map(terminal::Outcome::retain)
            .map_or(
                service::EpochAccess::Unknown,
                service::EpochAccess::Terminal,
            )
    }
}

const _: () = assert!(
    std::mem::size_of::<Gate<'static, 'static, 'static, 'static, 'static, 0, core::Publish>>()
        == std::mem::size_of::<
            core::Core<'static, 'static, 'static, 'static, 'static, 0, core::Publish>,
        >() + std::mem::size_of::<conn::Ready<'static, 0>>()
);
