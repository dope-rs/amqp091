use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::channel::{receipt, route};
use crate::adapter::publish::request;
use crate::adapter::{self, core, step};
use crate::failure;
use crate::service::cancel;
use crate::state::channel::cancel::lanes;

mod active;
mod header;
pub(super) mod method;
mod stream;

enum Stage<
    'exchange,
    'key,
    'properties,
    'body,
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'b,
    const CONFIRM: bool,
    const ID: u8,
> where
    method::Kind<CONFIRM>: method::Mode<'o, 'd, ID>,
{
    Method(method::Method<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>),
    Header(header::Header<'properties, 'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>),
    Body(active::Active<'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>),
}

pub(super) struct Active<
    'exchange,
    'key,
    'properties,
    'body,
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'b,
    const CONFIRM: bool,
    const ID: u8,
> where
    method::Kind<CONFIRM>: method::Mode<'o, 'd, ID>,
{
    phase: Stage<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>,
}

enum SendPhase<
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
    Active(Active<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, false, ID>),
    Done,
}

pub(super) enum Poll<A, T, E, W> {
    Pending(A),
    Ready(T, W),
    Error(E),
}

#[must_use = "a publish send must be polled to completion"]
pub struct Send<
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
    phase: SendPhase<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>,
}

impl<
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'exchange,
    'key,
    'properties,
    'body,
    'd: 'b,
    const CONFIRM: bool,
    const ID: u8,
> Active<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>
where
    method::Kind<CONFIRM>: method::Mode<'o, 'd, ID>,
{
    pub(super) const fn new(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        input: request::Checked<'exchange, 'key, 'properties, 'body>,
    ) -> Self {
        Self {
            phase: Stage::Method(method::Method::new(route, input)),
        }
    }

    pub(super) fn cancel(&mut self) {
        match &mut self.phase {
            Stage::Method(method) => method.cancel(),
            Stage::Header(header) => header.stream.cancel(),
            Stage::Body(body) => body.stream.cancel(),
        }
    }
}

fn advance<
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'exchange,
    'key,
    'properties,
    'body,
    'd: 'b,
    T,
    const CONFIRM: bool,
    const ID: u8,
>(
    active: Active<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>,
    mut cx: local::Access<'_, region::Identity<'d>>,
    mut wake: wake::Waker<'d>,
    finish: impl FnOnce(
        stream::Stream<'o, 'r, 'c, 'b, 'd, CONFIRM, ID>,
        local::Access<'_, region::Identity<'d>>,
        stream::Last<'c>,
    ) -> T,
) -> Poll<
    Active<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>,
    T,
    failure::Runtime<'c, 'b, 'd, ID>,
    wake::Waker<'d>,
>
where
    method::Kind<CONFIRM>: method::Mode<'o, 'd, ID>,
{
    let mut phase = active.phase;
    loop {
        phase = match phase {
            Stage::Method(mut method) => match method.poll::<CONFIRM>(cx.reborrow(), wake) {
                step::Step::Pending => {
                    return Poll::Pending(Active {
                        phase: Stage::Method(method),
                    });
                }
                step::Step::Error(error) => {
                    method.cancel();
                    return Poll::Error(error);
                }
                step::Step::Ready(start, next) => {
                    wake = next;
                    Stage::Header(method.next(start))
                }
            },
            Stage::Header(mut header) => match header.poll(cx.reborrow(), wake) {
                step::Step::Pending => {
                    return Poll::Pending(Active {
                        phase: Stage::Header(header),
                    });
                }
                step::Step::Error(error) => {
                    header.stream.abort(cx);
                    return Poll::Error(error);
                }
                step::Step::Ready(header::Next::Body(body), next) => {
                    wake = next;
                    Stage::Body(header.body(body))
                }
                step::Step::Ready(header::Next::Last(last), next) => {
                    return Poll::Ready(finish(header.stream, cx.reborrow(), last), next);
                }
            },
            Stage::Body(mut body) => match body.poll(cx.reborrow(), wake) {
                step::Step::Pending => {
                    return Poll::Pending(Active {
                        phase: Stage::Body(body),
                    });
                }
                step::Step::Error(error) => {
                    body.stream.abort(cx);
                    return Poll::Error(error);
                }
                step::Step::Ready(active::Next::More, next) => {
                    wake = next;
                    Stage::Body(body)
                }
                step::Step::Ready(active::Next::Last(last), next) => {
                    return Poll::Ready(finish(body.stream, cx.reborrow(), last), next);
                }
            },
        };
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'exchange, 'key, 'properties, 'body, 'd: 'b, const ID: u8>
    Active<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, false, ID>
{
    pub(super) fn poll(
        self,
        cx: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> Poll<Self, (), failure::Runtime<'c, 'b, 'd, ID>, wake::Waker<'d>> {
        advance(self, cx, wake, stream::Stream::finish)
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'exchange, 'key, 'properties, 'body, 'd: 'b, const ID: u8>
    Active<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, true, ID>
{
    pub(super) fn poll(
        self,
        cx: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> Poll<
        Self,
        (
            receipt::Receipt<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
            cancel::Ticket<'o, 'd, lanes::Confirm, ID>,
        ),
        failure::Runtime<'c, 'b, 'd, ID>,
        wake::Waker<'d>,
    > {
        advance(self, cx, wake, |stream, cx, last| {
            let route = stream.route();
            (route.receipt(), stream.finish(cx, last))
        })
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'exchange, 'key, 'properties, 'body, 'd: 'b, const ID: u8>
    Send<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>
{
    pub(super) const fn new(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        input: request::Checked<'exchange, 'key, 'properties, 'body>,
    ) -> Self {
        Self {
            phase: SendPhase::Active(Active::new(route, input)),
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'exchange, 'key, 'properties, 'body, 'd: 'b, const ID: u8>
    adapter::Poll<'c, 'b, 'd, ID>
    for Send<'exchange, 'key, 'properties, 'body, 'o, 'r, 'c, 'b, 'd, ID>
{
    type Output = ();

    fn poll(
        &mut self,
        cx: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), failure::Runtime<'c, 'b, 'd, ID>>> {
        let active = match mem::replace(&mut self.phase, SendPhase::Done) {
            SendPhase::Active(active) => active,
            SendPhase::Done => {
                return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Polled)));
            }
        };
        match active.poll(cx, wake) {
            Poll::Pending(active) => {
                self.phase = SendPhase::Active(active);
                task::Poll::Pending
            }
            Poll::Ready((), _) => task::Poll::Ready(Ok(())),
            Poll::Error(error) => task::Poll::Ready(Err(error)),
        }
    }
}

impl<const ID: u8> Drop for Send<'_, '_, '_, '_, '_, '_, '_, '_, '_, ID> {
    fn drop(&mut self) {
        if let SendPhase::Active(active) = &mut self.phase {
            active.cancel();
        }
    }
}

const _: () = assert!(
    mem::size_of::<Send<'_, '_, '_, '_, '_, '_, '_, '_, '_, 0>>() == 15 * mem::size_of::<usize>()
);
