use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::channel::route;
use crate::adapter::publish::{request, send};
use crate::adapter::{self, core};
use crate::{failure, protocol};

mod wait;

enum Phase<'exchange, 'key, 'properties, 'body, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    Active(send::Active<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, true, ID>),
    Wait(wait::Wait<'o, 'r, 'c, 'b, 'd, ID>),
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
        let wait = match mem::replace(&mut self.phase, Phase::Done) {
            Phase::Active(active) => match active.poll(cx.reborrow(), wake) {
                send::Poll::Pending(active) => {
                    self.phase = Phase::Active(active);
                    return task::Poll::Pending;
                }
                send::Poll::Ready((entry, ticket), next) => {
                    wake = next;
                    wait::Wait::new(entry, ticket)
                }
                send::Poll::Error(error) => return task::Poll::Ready(Err(error)),
            },
            Phase::Wait(wait) => wait,
            Phase::Done => {
                return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Polled)));
            }
        };
        match wait.poll(cx, wake) {
            wait::Poll::Pending(wait) => {
                self.phase = Phase::Wait(wait);
                task::Poll::Pending
            }
            wait::Poll::Ready(result) => task::Poll::Ready(result),
        }
    }
}

impl<const ID: u8> Drop for Confirm<'_, '_, '_, '_, '_, '_, '_, '_, '_, ID> {
    fn drop(&mut self) {
        match &self.phase {
            Phase::Active(active) => active.cancel(),
            Phase::Wait(wait) => wait.ticket.cancel(),
            Phase::Done => {}
        }
    }
}

const _: () = assert!(
    mem::size_of::<
        Confirm<'static, 'static, 'static, 'static, 'static, 'static, 'static, 'static, 'static, 0>,
    >() == 19 * mem::size_of::<usize>()
);
