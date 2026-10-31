use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::channel::route;
use crate::adapter::publish::request;
use crate::adapter::publish::send::{header, stream};
use crate::adapter::{core, step};
use crate::failure;
use crate::failure::wire::{Allocation, Fault, Resident};
use crate::protocol::method::{self, basic};
use crate::service::cancel::pending::reap;
use crate::service::cancel::{self};
use crate::state::channel::cancel::lanes;
use crate::state::channel::facets;
use crate::state::channel::publish::confirms;
use crate::state::channels;

pub(in crate::adapter::publish) struct Kind<const CONFIRM: bool>;

pub(in crate::adapter::publish) trait Mode<'o, 'd: 'o, const ID: u8> {
    type Ticket;

    fn begin<'r, 'c: 'r, 'b: 'c, 'a>(
        state: facets::Publish<'a, 'c, 'b, 'd, ID>,
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        index: channels::Index<'_, ID>,
        slot: lanes::Slot<lanes::Publish>,
    ) -> Result<Option<Tickets<'o, 'd, Self::Ticket, ID>>, failure::Error>
    where
        'd: 'a;

    fn cancel(send: &cancel::Ticket<'o, 'd, lanes::Publish, ID>, ack: &Self::Ticket);
}

impl<'o, 'd: 'o, const ID: u8> Mode<'o, 'd, ID> for Kind<false> {
    type Ticket = ();

    fn begin<'r, 'c: 'r, 'b: 'c, 'a>(
        state: facets::Publish<'a, 'c, 'b, 'd, ID>,
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        index: channels::Index<'_, ID>,
        slot: lanes::Slot<lanes::Publish>,
    ) -> Result<Option<Tickets<'o, 'd, (), ID>>, failure::Error>
    where
        'd: 'a,
    {
        if state.control.has_confirms() {
            return Err(failure::Error::Protocol(Fault::ConfirmPublishRequired));
        }
        let flight = state.flight();
        let Some(vacant) = flight.vacant() else {
            return Ok(None);
        };
        let ticket = vacant.begin(|key| {
            route
                .core()
                .cancel()
                .issue(index, route.lease().raw(), slot, key)
        })?;
        Ok(Some((ticket, ())))
    }

    fn cancel(send: &cancel::Ticket<'o, 'd, lanes::Publish, ID>, (): &()) {
        send.cancel();
    }
}

impl<'o, 'd: 'o, const ID: u8> Mode<'o, 'd, ID> for Kind<true> {
    type Ticket = cancel::Ticket<'o, 'd, lanes::Confirm, ID>;

    fn begin<'r, 'c: 'r, 'b: 'c, 'a>(
        state: facets::Publish<'a, 'c, 'b, 'd, ID>,
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        index: channels::Index<'_, ID>,
        slot: lanes::Slot<lanes::Publish>,
    ) -> Result<Option<Tickets<'o, 'd, Self::Ticket, ID>>, failure::Error>
    where
        'd: 'a,
    {
        let (conf, flight) = state.split();
        let Some(vacant) = flight.vacant() else {
            return Ok(None);
        };
        let core = route.core();
        let (send, ack) = conf.try_reserve(|confirm, key| {
            let ack = core
                .cancel()
                .issue(index, route.lease().raw(), confirm, key)?;
            match vacant.begin(|key| core.cancel().issue(index, route.lease().raw(), slot, key)) {
                Ok(send) => Ok((send, ack)),
                Err(error) => {
                    ack.complete();
                    Err(error)
                }
            }
        })?;
        Ok(Some((send, ack)))
    }

    fn cancel(send: &cancel::Ticket<'o, 'd, lanes::Publish, ID>, ack: &Self::Ticket) {
        send.cancel();
        ack.cancel();
    }
}

enum Defer<'c> {
    Wait(fifo::Index<'c>),
}

pub(super) struct Method<
    'exchange,
    'key,
    'properties,
    'body,
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'b,
    const ID: u8,
> {
    route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
    input: request::Checked<'exchange, 'key, 'properties, 'body>,
    credit: queue::Registration<'o, 'd, fifo::Scope<'c>>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'exchange, 'key, 'properties, 'body, 'd: 'b, const ID: u8>
    Method<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>
{
    pub(super) const fn new(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        input: request::Checked<'exchange, 'key, 'properties, 'body>,
    ) -> Self {
        Self {
            route,
            input,
            credit: queue::Registration::new(),
        }
    }

    pub(super) fn poll<const CONFIRM: bool>(
        &mut self,
        mut access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> step::Step<'c, 'b, 'd, stream::Started<'o, 'd, CONFIRM, ID>, ID>
    where
        Kind<CONFIRM>: Mode<'o, 'd, ID>,
    {
        let route = self.route;
        let core = route.core();
        let admit = route.try_frame(
            access.reborrow(),
            |output| {
                method::Encode::write(
                    &basic::Publish {
                        exchange: self.input.exchange.into(),
                        routing_key: self.input.key,
                        mandatory: self.input.mandatory,
                        immediate: false,
                    },
                    output,
                    route.lease().id(),
                );
            },
            |view, _, ()| {
                let blocked =
                    view.connection
                        .try_cause()
                        .map_err(|error| failure::Error::Resident {
                            target: Allocation::Short,
                            error: Resident::Retain(error),
                        })?;
                if let Some(reason) = blocked {
                    return Err(core::Reject::Error(failure::Error::ConnectionBlocked(
                        reason.into(),
                    )));
                }
                if view.channel.is_retiring() {
                    return Err(failure::Error::Closed.into());
                }
                if CONFIRM {
                    core.cancel().reap::<lanes::Confirm>(
                        view.index,
                        route.lease().raw(),
                        |item| {
                            if matches!(
                                view.channel.confirm().reap(item.identity()),
                                confirms::Reap::Removed
                            ) {
                                reap::Decision::Remove
                            } else {
                                reap::Decision::Keep
                            }
                        },
                    )?;
                }
                let slot = lanes::Slot::<lanes::Publish>::ZERO;
                let state = view.channel.publish();
                if !state.control.is_flow() {
                    return Err(failure::Error::ChannelFlowPaused.into());
                }
                if state.is_waiting() {
                    return Err(core::Reject::Defer(Defer::Wait(view.index.lane())));
                }
                let Some((ticket, conf)) =
                    <Kind<CONFIRM> as Mode<'o, 'd, ID>>::begin(state, route, view.index, slot)?
                else {
                    return Err(core::Reject::Defer(Defer::Wait(view.index.lane())));
                };
                Ok(stream::Started::<CONFIRM, ID>::new(
                    ticket,
                    conf,
                    self.input.max(),
                ))
            },
        );
        let admit = match admit {
            Ok(admit) => admit,
            Err(Defer::Wait(index)) => {
                return step::Step::channel(route, access, index, &mut self.credit, wake);
            }
        };
        step::Step::admit(route, access, admit, &mut self.credit, wake)
    }

    pub(super) fn next<const CONFIRM: bool>(
        self,
        start: stream::Started<'o, 'd, CONFIRM, ID>,
    ) -> header::Header<'properties, 'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>
    where
        Kind<CONFIRM>: Mode<'o, 'd, ID>,
    {
        header::Header::new(
            stream::Stream::new(self.route, start),
            self.input.properties,
            self.input.body,
            self.credit,
        )
    }
}

const _: () = {
    use std::mem;

    assert!(
        mem::size_of::<
            Method<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                0,
            >,
        >() == 14 * mem::size_of::<usize>()
    );
};

type Tickets<'o, 'd, T, const ID: u8> = (cancel::Ticket<'o, 'd, lanes::Publish, ID>, T);
