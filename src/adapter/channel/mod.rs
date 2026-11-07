use std::{marker, mem, task};

use dope::core::driver::schedule::ready::wake::{fanout, queue, table};

mod open;
pub(in crate::adapter) mod receipt;
mod returned;
pub(in crate::adapter) mod route;

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;
pub use open::Open;
pub use returned::Returned;

use crate::adapter::core::egress;
use crate::adapter::{self, conn, core};
use crate::failure;
use crate::protocol::method::{self, channel};
use crate::service::cancel::pending::reap;
use crate::service::cancel::{self};
use crate::state::channel::cancel::lanes;
use crate::state::channel::returns;
use crate::state::{channels, shared};

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
/// An exact channel generation branded to one port, driver lifetime and route.
pub struct Lease<'c, 'd, const ID: u8> {
    raw: channels::Lease<'d, ID>,
    scope: marker::PhantomData<fn(&'c ()) -> &'c ()>,
}

pub struct Reservation<'o, 'c: 'o, 'd: 'c, const ID: u8> {
    cancellation:
        o3::mem::Lease<cancel::Ticket<'o, 'd, lanes::Reservation, ID>, o3::mem::lease::Open>,
    scope: marker::PhantomData<fn(&'c ()) -> &'c ()>,
}

impl<'o, 'c: 'o, 'd: 'c, const ID: u8> Reservation<'o, 'c, 'd, ID> {
    /// Commits the unique reservation and retires its cancellation authority.
    pub fn commit(self) -> Lease<'c, 'd, ID> {
        let (ticket, ()) = self.cancellation.into_parts();
        let raw = ticket.identity();
        ticket.complete();
        Lease::from_raw(raw)
    }

    pub fn cancel(self) {
        let (ticket, ()) = self.cancellation.into_parts();
        ticket.cancel();
    }

    pub fn id(&self) -> u16 {
        self.cancellation.sink().identity().id()
    }

    pub fn lease(&self) -> Lease<'c, 'd, ID> {
        Lease::from_raw(self.cancellation.sink().identity())
    }

    pub fn open<'r, 'b: 'c>(
        self,
        amqp: adapter::Adapter<'o, 'r, 'c, 'b, 'd, ID>,
        mut access: local::Access<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
    ) -> Result<Open<'o, 'r, 'c, 'b, 'd, ID>, failure::Runtime<'c, 'b, 'd, ID>> {
        match amqp
            .methods()
            .reserve(access.reborrow(), target, self.lease(), channel::Open)
        {
            Ok(call) => Ok(Open::new(amqp, self, call)),
            Err(error) => {
                amqp.channels().release(access, self);
                Err(error)
            }
        }
    }
}

impl<'c, 'd, const ID: u8> Lease<'c, 'd, ID> {
    pub(crate) const fn from_raw(raw: channels::Lease<'d, ID>) -> Self {
        Self {
            raw,
            scope: marker::PhantomData,
        }
    }

    pub(crate) const fn raw(self) -> channels::Lease<'d, ID> {
        self.raw
    }

    pub const fn id(self) -> u16 {
        self.raw.id()
    }
}

#[repr(transparent)]
pub struct Channels<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Channels>,
}

impl<const ID: u8> Copy for Channels<'_, '_, '_, '_, '_, ID> {}

impl<const ID: u8> Clone for Channels<'_, '_, '_, '_, '_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Channels<'o, 'r, 'c, 'b, 'd, ID> {
    pub(super) const fn new(core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Channels>) -> Self {
        Self { core }
    }

    pub fn reserve(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        id: u16,
    ) -> Result<Reservation<'o, 'c, 'd, ID>, failure::Runtime<'c, 'b, 'd, ID>> {
        match core::Gate::new(self.core, target).with(
            access,
            |state| -> Result<Reservation<'o, 'c, 'd, ID>, failure::Error> {
                self.reap(state, Some(id))?;
                state.routes.reserve(
                    id,
                    state.connection.negotiated().channel_max,
                    |index, raw| {
                        Ok(Reservation {
                            cancellation: o3::mem::Lease::open(
                                self.core.cancel().issue(
                                    index,
                                    raw,
                                    lanes::Slot::<lanes::Reservation>::ZERO,
                                    raw,
                                )?,
                                (),
                            ),
                            scope: marker::PhantomData,
                        })
                    },
                )
            },
        ) {
            crate::service::EpochAccess::Live(Ok(slot)) => Ok(slot),
            crate::service::EpochAccess::Live(Err(error)) => Err(failure::Runtime::from(error)),
            crate::service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            crate::service::EpochAccess::Unknown => {
                Err(failure::Runtime::from(failure::Error::Closed))
            }
        }
    }

    pub fn next(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
    ) -> Result<Reservation<'o, 'c, 'd, ID>, failure::Runtime<'c, 'b, 'd, ID>> {
        match core::Gate::new(self.core, target).with(
            access,
            |state| -> Result<Reservation<'o, 'c, 'd, ID>, failure::Error> {
                self.reap(state, None)?;
                state
                    .routes
                    .next(state.connection.negotiated().channel_max, |index, raw| {
                        Ok(Reservation {
                            cancellation: o3::mem::Lease::open(
                                self.core.cancel().issue(
                                    index,
                                    raw,
                                    lanes::Slot::<lanes::Reservation>::ZERO,
                                    raw,
                                )?,
                                (),
                            ),
                            scope: marker::PhantomData,
                        })
                    })
            },
        ) {
            crate::service::EpochAccess::Live(Ok(slot)) => Ok(slot),
            crate::service::EpochAccess::Live(Err(error)) => Err(failure::Runtime::from(error)),
            crate::service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            crate::service::EpochAccess::Unknown => {
                Err(failure::Runtime::from(failure::Error::Closed))
            }
        }
    }

    fn reap(
        self,
        state: &mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>,
        id: Option<u16>,
    ) -> Result<(), failure::Error> {
        match id {
            Some(id) => {
                let Some(lease) = state.routes.lease(id) else {
                    return Ok(());
                };
                let Some(index) = state.routes.project(lease, |route| route.index) else {
                    return Ok(());
                };
                self.core
                    .cancel()
                    .reap::<lanes::Reservation>(index, lease, |item| {
                        state.routes.collect(item.lease());
                        reap::Decision::Remove
                    })?;
            }
            None => self.core.cancel().reap_any::<lanes::Reservation>(|item| {
                state.routes.collect(item.lease());
                reap::Decision::Remove
            }),
        }
        Ok(())
    }

    pub fn current(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        id: u16,
    ) -> Option<Lease<'c, 'd, ID>> {
        self.core
            .shared()
            .write(access, |state| state.routes.lease(id).map(Lease::from_raw))
    }

    pub fn release(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        slot: Reservation<'o, 'c, 'd, ID>,
    ) {
        let lease = slot.commit();
        self.core.shared().write(access, |state| {
            state.routes.collect(lease.raw());
        });
    }

    /// Waits for exact channel order and transport credit while the caller
    /// retains the semantic method value.
    /// After `Pending`, retain `credit` for the next poll or pass it to
    /// [`Self::cancel_send`] before abandoning the operation.
    pub fn poll_send<M: method::Encode>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: conn::Epoch<'d, ID>,
        lease: Lease<'c, 'd, ID>,
        method: &M,
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, ID>>> {
        egress::Egress::new(self.core).poll(
            access,
            target.raw(),
            lease,
            |output| method.write(output, lease.id()),
            credit,
            wake,
        )
    }

    /// Cancels a pending send and wakes at most one successor in its original
    /// channel or transport-credit lane. The wake is a retry hint; its recipient
    /// must recheck admission.
    pub fn cancel_send(self, credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>) {
        self.core.permits().cancel(credit);
    }

    pub fn flow_active(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        lease: Lease<'c, 'd, ID>,
    ) -> Result<Option<bool>, failure::Runtime<'c, 'b, 'd, ID>> {
        match core::Gate::new(self.core, target).with(access, |state| {
            state
                .routes
                .project(lease.raw(), |route| {
                    (!route.channel.is_retiring()).then(|| route.channel.control().is_flow())
                })
                .flatten()
        }) {
            crate::service::EpochAccess::Live(value) => Ok(value),
            crate::service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            crate::service::EpochAccess::Unknown => {
                Err(failure::Runtime::from(failure::Error::Closed))
            }
        }
    }

    pub fn next_return(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        lease: Lease<'c, 'd, ID>,
        wait: &mut table::Registration<'o, 'd>,
        wake: wake::Waker<'d>,
    ) -> Result<Option<Returned<'c, 'b, 'd>>, failure::Runtime<'c, 'b, 'd, ID>> {
        let stores = self.core.stores();
        let next = core::Gate::new(self.core, target).with(access, |state| {
            state.routes.project(lease.raw(), |route| {
                route.channel.content(stores).next_return(wait, wake)
            })
        });
        if !matches!(
            next,
            crate::service::EpochAccess::Live(Some(Ok(returns::Next::Pending)))
        ) {
            wait.clear();
        }
        match next {
            crate::service::EpochAccess::Live(Some(Ok(returns::Next::Item {
                returned: item,
                freed,
            }))) => {
                self.core
                    .wake_route(target.epoch().raw(), freed)
                    .map_err(failure::Runtime::from)?;
                Ok(Some(item))
            }
            crate::service::EpochAccess::Live(Some(Ok(returns::Next::Pending))) => Ok(None),
            crate::service::EpochAccess::Live(Some(Ok(returns::Next::Closed))) => {
                Err(failure::Runtime::from(failure::Error::Closed))
            }
            crate::service::EpochAccess::Live(Some(Err(error))) => {
                Err(failure::Runtime::from(failure::Error::Table(error)))
            }
            crate::service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            crate::service::EpochAccess::Live(None) | crate::service::EpochAccess::Unknown => {
                Err(failure::Runtime::from(failure::Error::Closed))
            }
        }
    }

    pub fn watch_flow(
        self,
        wait: &mut fanout::Registration<'o, 'd>,
        wake: &wake::Waker<'d>,
    ) -> Result<(), failure::Error> {
        self.core.waiters().watch_flow(wait, wake)
    }
}

const _: () = assert!(
    mem::size_of::<Channels<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<usize>()
);
const _: () = assert!(
    mem::size_of::<Lease<'static, 'static, 0>>() == mem::size_of::<channels::Lease<'static, 0>>()
);
const _: () = assert!(
    mem::align_of::<Lease<'static, 'static, 0>>() == mem::align_of::<channels::Lease<'static, 0>>()
);
const _: () = assert!(
    mem::size_of::<Reservation<'static, 'static, 'static, 0>>() == 4 * mem::size_of::<usize>()
);
