use std::task;

use dope_amqp091::adapter::channel::Lease;
use dope_amqp091::adapter::{Poll as _, conn, method, publish};
use dope_amqp091::message::property::Properties;
use dope_amqp091::protocol::method::{channel, confirm};
use dope_amqp091::{adapter, failure, protocol};
use dope_fiber::abi::Fiber;
use dope_fiber::context;
use dope_test::alloc::Tracker;
use o3::base::cell::local;

#[derive(Debug)]
pub enum Error<E> {
    Runtime(E),
    Allocations { count: usize, bytes: usize },
    Confirmation(protocol::Confirmation),
}

enum Phase<'properties, 'o, 'r, 'c, 'b, 'd> {
    Open(
        Option<(
            Lease<'c, 'd, 0>,
            method::Call<'o, 'r, 'c, 'b, 'd, 0, channel::Open>,
        )>,
    ),
    Select(
        Lease<'c, 'd, 0>,
        Option<method::Call<'o, 'r, 'c, 'b, 'd, 0, confirm::Select>>,
    ),
    Publish(
        Lease<'c, 'd, 0>,
        Option<
            publish::Confirm<
                'properties,
                'properties,
                'properties,
                'properties,
                'o,
                'r,
                'c,
                'b,
                'd,
                0,
            >,
        >,
    ),
}

pub struct Commit<'properties, 'o, 'r, 'c, 'b, 'd> {
    amqp: adapter::Adapter<'o, 'r, 'c, 'b, 'd, 0>,
    target: conn::Ready<'d, 0>,
    props: &'properties Properties,
    phase: Phase<'properties, 'o, 'r, 'c, 'b, 'd>,
}

impl<'properties, 'o, 'r, 'c, 'b, 'd> Commit<'properties, 'o, 'r, 'c, 'b, 'd> {
    pub fn new(
        amqp: adapter::Adapter<'o, 'r, 'c, 'b, 'd, 0>,
        target: conn::Ready<'d, 0>,
        props: &'properties Properties,
    ) -> Self {
        Self {
            amqp,
            target,
            props,
            phase: Phase::Open(None),
        }
    }
}

impl<'properties, 'o, 'r, 'c, 'b, 'd> Fiber<'d> for Commit<'properties, 'o, 'r, 'c, 'b, 'd> {
    type Output = Result<(), Error<failure::Runtime<'c, 'b, 'd, 0>>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        loop {
            let wake = cx.as_ref().completion();
            let mut local = local::Access::new(cx.as_mut().region_token());
            match &mut this.phase {
                Phase::Open(slot) => {
                    let (lease, call) = match slot {
                        Some(current) => current,
                        slot @ None => {
                            let lease = this
                                .amqp
                                .channels()
                                .reserve(local.reborrow(), this.target, 1)
                                .map_err(Error::Runtime)?
                                .commit();
                            let call = this
                                .amqp
                                .methods()
                                .reserve(local.reborrow(), this.target, lease, channel::Open)
                                .map_err(Error::Runtime)?;
                            slot.insert((lease, call))
                        }
                    };
                    task::ready!(call.poll(local, wake)).map_err(Error::Runtime)?;
                    this.phase = Phase::Select(*lease, None);
                }
                Phase::Select(lease, slot) => {
                    let call = match slot {
                        Some(call) => call,
                        slot @ None => slot.insert(
                            this.amqp
                                .methods()
                                .reserve(local.reborrow(), this.target, *lease, confirm::Select)
                                .map_err(Error::Runtime)?,
                        ),
                    };
                    task::ready!(call.poll(local, wake)).map_err(Error::Runtime)?;
                    this.phase = Phase::Publish(*lease, None);
                }
                Phase::Publish(lease, slot) => {
                    let send = match &mut *slot {
                        Some(send) => send,
                        slot @ None => slot.insert(
                            this.amqp
                                .publish()
                                .confirm(
                                    local.read(),
                                    this.target,
                                    *lease,
                                    publish::Request {
                                        exchange: "",
                                        routing_key: "committed",
                                        mandatory: false,
                                        properties: this.props,
                                        body: b"x",
                                    },
                                )
                                .map_err(Error::Runtime)?,
                        ),
                    };
                    let (out, alloc) = Tracker::usage(|| send.poll(local, wake));
                    if alloc != (0, 0) {
                        return task::Poll::Ready(Err(Error::Allocations {
                            count: alloc.0,
                            bytes: alloc.1,
                        }));
                    }
                    let outcome = task::ready!(out).map_err(Error::Runtime)?;
                    if outcome != protocol::Confirmation::Ack {
                        return task::Poll::Ready(Err(Error::Confirmation(outcome)));
                    }
                    *slot = None;
                    return task::Poll::Ready(Ok(()));
                }
            }
        }
    }
}
