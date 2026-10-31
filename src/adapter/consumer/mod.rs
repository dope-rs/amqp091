use std::mem::size_of;
use std::task;

mod delivery;
mod error;
mod lease;
mod recv;
mod settle;

pub use delivery::Delivery;
use dope::core::driver::schedule::ready::wake;
pub use error::Error;
pub use lease::Lease;
use o3::base::cell::{local, region};
use o3::mem::lease::Open;
use o3::mem::{self};
pub use recv::Recv;
pub use settle::Settle;

use crate::adapter::channel::route::Route;
use crate::adapter::{channel, conn, core};
use crate::failure;
use crate::message::ack;
use crate::protocol::wire::text::resident;
use crate::protocol::{self};
use crate::service::cancel;
use crate::state::channel::cancel::lanes;
use crate::state::channel::{Freed, Phase, consumer};

/// The exact generation and channel authority observed when a delivery leaves
/// the shared consumer queue.
#[doc(hidden)]
pub struct Origin<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    epoch: conn::Epoch<'d, ID>,
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Consumers>,
    lease: channel::Lease<'c, 'd, ID>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Origin<'o, 'r, 'c, 'b, 'd, ID> {
    const fn new(
        epoch: conn::Epoch<'d, ID>,
        core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Consumers>,
        lease: channel::Lease<'c, 'd, ID>,
    ) -> Self {
        Self { epoch, core, lease }
    }

    pub(super) const fn epoch(&self) -> conn::Epoch<'d, ID> {
        self.epoch
    }

    pub(super) const fn core(&self) -> core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Consumers> {
        self.core
    }

    pub(super) const fn lease(&self) -> channel::Lease<'c, 'd, ID> {
        self.lease
    }
}

/// Selects the minimum delivery authority required by an ACK mode.
#[doc(hidden)]
pub trait Mode: ack::Mode {
    #[doc(hidden)]
    type Source<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>;

    #[doc(hidden)]
    fn bind<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>(
        origin: Origin<'o, 'r, 'c, 'b, 'd, ID>,
    ) -> Self::Source<'o, 'r, 'c, 'b, 'd, ID>;
}

impl Mode for ack::Auto {
    type Source<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> = ack::Auto;

    fn bind<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>(
        origin: Origin<'o, 'r, 'c, 'b, 'd, ID>,
    ) -> Self::Source<'o, 'r, 'c, 'b, 'd, ID> {
        let Origin {
            epoch: _,
            core: _,
            lease: _,
        } = origin;
        ack::Auto
    }
}

impl Mode for ack::Manual {
    type Source<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> = Origin<'o, 'r, 'c, 'b, 'd, ID>;

    fn bind<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>(
        origin: Origin<'o, 'r, 'c, 'b, 'd, ID>,
    ) -> Self::Source<'o, 'r, 'c, 'b, 'd, ID> {
        origin
    }
}

/// Coherent point-in-time state for one invariant-branded consumer.
#[derive(PartialEq, Eq)]
pub struct Snapshot<'b, 'd: 'b> {
    pub generation: u64,
    pub buffered_deliveries: u64,
    pub queue: resident::Short<'b, 'd>,
}

/// Invariant driver- and route-branded consumer key.
pub struct Key<'o, 'd: 'o, const ID: u8> {
    cancellation: mem::Lease<cancel::Ticket<'o, 'd, lanes::Consumer, ID>, Open>,
}

impl<'o, 'd: 'o, const ID: u8> Key<'o, 'd, ID> {
    pub(super) const fn from_cancel(ticket: cancel::Ticket<'o, 'd, lanes::Consumer, ID>) -> Self {
        Self {
            cancellation: mem::Lease::open(ticket, ()),
        }
    }

    pub(crate) fn raw(&self) -> consumer::Key {
        self.cancellation.sink().identity()
    }

    pub(super) fn complete(self) {
        let (ticket, ()) = self.cancellation.into_parts();
        ticket.complete();
    }
}

#[repr(transparent)]
pub struct Consumers<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Consumers>,
}

impl<const ID: u8> Copy for Consumers<'_, '_, '_, '_, '_, ID> {}

impl<const ID: u8> Clone for Consumers<'_, '_, '_, '_, '_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Consumers<'o, 'r, 'c, 'b, 'd, ID> {
    pub(super) const fn new(core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Consumers>) -> Self {
        Self { core }
    }

    pub fn try_bind<M: Mode>(
        self,
        ready: conn::Ready<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        key: protocol::ConsumerKey<Key<'o, 'd, ID>, M>,
    ) -> Result<Lease<'o, 'r, 'c, 'b, 'd, ID, M>, failure::Error> {
        self.core
            .cancel()
            .try_bind(key.inner().cancellation.sink(), lease.raw())?;
        Ok(Lease::new(self, lease, key, ready))
    }

    pub(super) fn poll<M: Mode>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        key: &protocol::ConsumerKey<Key<'o, 'd, ID>, M>,
        wake: wake::Waker<'d>,
    ) -> Result<
        task::Poll<Option<Delivery<'o, 'r, 'c, 'b, 'd, ID, M>>>,
        failure::Runtime<'c, 'b, 'd, ID>,
    > {
        use consumer::delivery::Next;

        let key = key.inner();
        let route = Route::new(self.core, target, lease);
        let origin = Origin::new(target.epoch(), route.core(), route.lease());
        let store = self.core.stores().deliveries();
        let view = route.with(access, |route| {
            match route.channel.consumers().next(key.raw(), wake, store) {
                Next::Unavailable => None,
                Next::Pending => Some((task::Poll::Pending, Freed::Available)),
                Next::Closed => {
                    if route.channel.control().phase == Phase::Closed {
                        return None;
                    }
                    let freed = match route.channel.consumers().detach(key.raw(), store) {
                        consumer::Removal::Removed(freed) => freed,
                        consumer::Removal::Missing => Freed::Available,
                    };
                    Some((task::Poll::Ready(None), freed))
                }
                Next::Delivery {
                    delivery: item,
                    freed,
                } => Some((task::Poll::Ready(Some(Delivery::new(item, origin))), freed)),
            }
        });
        match view {
            crate::service::EpochAccess::Live(Some(Some((poll, freed)))) => {
                match freed {
                    Freed::Available => {}
                    Freed::Route => self
                        .core
                        .wake_route(target.epoch().raw())
                        .map_err(failure::Runtime::from)?,
                }
                Ok(poll)
            }
            crate::service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            crate::service::EpochAccess::Live(Some(None) | None)
            | crate::service::EpochAccess::Unknown => {
                Err(failure::Runtime::from(failure::Error::Closed))
            }
        }
    }

    pub(super) fn snapshot<M>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        key: &protocol::ConsumerKey<Key<'o, 'd, ID>, M>,
    ) -> Result<Option<Snapshot<'b, 'd>>, failure::Runtime<'c, 'b, 'd, ID>> {
        let key = key.inner();
        match core::Gate::new(self.core, target).with(access, |state| {
            let epoch = state.connection.lifecycle.generation();
            state
                .routes
                .project(lease.raw(), |route| {
                    let Some((record, queued)) =
                        route.channel.consumers().read().snapshot(key.raw())
                    else {
                        return Ok(None);
                    };
                    let queue = match route
                        .topology
                        .graph()
                        .queue_entry(record.key(), record.name())
                    {
                        Some(entry) => entry.value().try_name()?,
                        None => record.try_name()?,
                    };
                    Ok(Some(Snapshot {
                        generation: epoch,
                        buffered_deliveries: queued,
                        queue,
                    }))
                })
                .transpose()
        }) {
            crate::service::EpochAccess::Live(state) => state
                .map(Option::flatten)
                .map_err(|error| failure::Runtime::from(failure::Error::Retain(error))),
            crate::service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            crate::service::EpochAccess::Unknown => {
                Err(failure::Runtime::from(failure::Error::Closed))
            }
        }
    }
}

const _: () = {
    assert!(
        size_of::<Origin<'static, 'static, 'static, 'static, 'static, 0>>()
            == 4 * size_of::<usize>()
    );
    assert!(size_of::<Key<'static, 'static, 0>>() == 4 * size_of::<usize>());
    assert!(
        size_of::<protocol::ConsumerKey<Key<'static, 'static, 0>, ack::Manual>>()
            == size_of::<Key<'static, 'static, 0>>()
    );
    assert!(
        size_of::<Option<protocol::ConsumerKey<Key<'static, 'static, 0>, ack::Manual>>>()
            == size_of::<protocol::ConsumerKey<Key<'static, 'static, 0>, ack::Manual>>()
    );
    assert!(
        size_of::<Consumers<'static, 'static, 'static, 'static, 'static, 0>>()
            == size_of::<usize>()
    );
};
