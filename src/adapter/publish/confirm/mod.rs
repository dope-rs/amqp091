use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::channel::{receipt, route};
use crate::adapter::publish::{request, send};
use crate::adapter::{self, core};
use crate::service::cancel;
use crate::state::channel::cancel::lanes;
use crate::{failure, protocol, service};

enum Phase<'exchange, 'key, 'properties, 'body, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    Active(send::Active<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, true, ID>),
    Wait {
        receipt: receipt::Receipt<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        ticket: cancel::Ticket<'o, 'd, lanes::Confirm, ID>,
    },
    Done,
}

#[must_use = "a confirmed publish must be polled to completion"]
pub struct Confirm<
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
    phase: Phase<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'exchange, 'key, 'properties, 'body, 'd: 'b, const ID: u8>
    Confirm<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>
{
    pub(in crate::adapter::publish) const fn new(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        input: request::Checked<'exchange, 'key, 'properties, 'body>,
    ) -> Self {
        Self {
            phase: Phase::Active(send::Active::new(route, input)),
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'exchange, 'key, 'properties, 'body, 'd: 'b, const ID: u8>
    adapter::Poll<'c, 'b, 'd, ID>
    for Confirm<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>
{
    type Output = protocol::Confirmation;

    fn poll(
        &mut self,
        mut cx: local::Access<'_, region::Identity<'d>>,
        mut wake: wake::Waker<'d>,
    ) -> task::Poll<Result<protocol::Confirmation, failure::Runtime<'c, 'b, 'd, ID>>> {
        let (receipt, ticket) = match mem::replace(&mut self.phase, Phase::Done) {
            Phase::Active(active) => match active.poll(cx.reborrow(), wake) {
                send::Poll::Pending(active) => {
                    self.phase = Phase::Active(active);
                    return task::Poll::Pending;
                }
                send::Poll::Ready(sent, next) => {
                    wake = next;
                    sent
                }
                send::Poll::Error(error) => return task::Poll::Ready(Err(error)),
            },
            Phase::Wait { receipt, ticket } => (receipt, ticket),
            Phase::Done => {
                return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Polled)));
            }
        };
        let result = receipt.with(cx.reborrow(), |view| {
            view.channel.confirm().poll(ticket.identity(), wake)
        });
        let out = match result {
            service::EpochAccess::Live(Ok(Some(item))) => {
                ticket.complete();
                receipt.collect(cx);
                return task::Poll::Ready(Ok(item));
            }
            service::EpochAccess::Live(Ok(None)) => {
                self.phase = Phase::Wait { receipt, ticket };
                return task::Poll::Pending;
            }
            service::EpochAccess::Live(Err(error)) => Err(error.branded()),
            service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            service::EpochAccess::Unknown => Err(failure::Runtime::Local(failure::Error::Closed)),
        };
        ticket.complete();
        task::Poll::Ready(out)
    }
}

impl<const ID: u8> Drop for Confirm<'_, '_, '_, '_, '_, '_, '_, '_, '_, ID> {
    fn drop(&mut self) {
        match &mut self.phase {
            Phase::Active(active) => active.cancel(),
            Phase::Wait { ticket, .. } => ticket.cancel(),
            Phase::Done => {}
        }
    }
}

const _: () = assert!(
    mem::size_of::<
        Confirm<'static, 'static, 'static, 'static, 'static, 'static, 'static, 'static, 'static, 0>,
    >() == 19 * mem::size_of::<usize>()
);
