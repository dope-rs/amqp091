use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::adapter::channel::route;
use crate::adapter::method::{send, wait};
use crate::adapter::{self, consumer, core};
use crate::failure;
use crate::protocol::method;
use crate::service::cancel;
use crate::state::channel::cancel::lanes;

enum Stage<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M> {
    Send {
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        send: send::Send<'o, 'c, 'd, ID, M>,
    },
    Wait(wait::Wait<'o, 'r, 'c, 'b, 'd, ID>),
    Done,
}

/// A method operation whose connection, channel, request, and reply cannot separate.
#[must_use = "a reserved method call must be polled or dropped"]
pub struct Call<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M> {
    stage: Stage<'o, 'r, 'c, 'b, 'd, ID, M>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M> Call<'o, 'r, 'c, 'b, 'd, ID, M> {
    pub(in crate::adapter) const fn new(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        ticket: cancel::Ticket<'o, 'd, lanes::Method, ID>,
        method: M,
    ) -> Self {
        Self {
            stage: Stage::Send {
                route,
                send: send::Send::new(ticket, method),
            },
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M> adapter::Poll<'c, 'b, 'd, ID>
    for Call<'o, 'r, 'c, 'b, 'd, ID, M>
where
    M: method::Op<'d>,
{
    type Output = M::Output<'b, consumer::Key<'o, 'd, ID>>;

    fn poll(
        &mut self,
        mut cx: local::Access<'_, region::Identity<'d>>,
        mut wake: wake::Waker<'d>,
    ) -> task::Poll<Result<Self::Output, failure::Runtime<'c, 'b, 'd, ID>>> {
        loop {
            let stage = mem::replace(&mut self.stage, Stage::Done);
            match stage {
                Stage::Send { route, send } => match send.poll(route, cx.reborrow(), wake) {
                    send::Poll::Pending(send) => {
                        self.stage = Stage::Send { route, send };
                        return task::Poll::Pending;
                    }
                    send::Poll::Error(error, send) => {
                        send.ticket.cancel();
                        return task::Poll::Ready(Err(error));
                    }
                    send::Poll::Ready(wait, next) => {
                        self.stage = Stage::Wait(wait);
                        wake = next;
                    }
                },
                Stage::Wait(wait) => match wait.poll::<M>(cx.reborrow(), wake) {
                    wait::Poll::Pending(wait) => {
                        self.stage = Stage::Wait(wait);
                        return task::Poll::Pending;
                    }
                    wait::Poll::Ready(output) => return task::Poll::Ready(output),
                },
                Stage::Done => {
                    return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Polled)));
                }
            }
        }
    }
}

impl<const ID: u8, M> Drop for Call<'_, '_, '_, '_, '_, ID, M> {
    fn drop(&mut self) {
        match &self.stage {
            Stage::Send { send, .. } => send.ticket.cancel(),
            Stage::Wait(wait) => wait.ticket.cancel(),
            Stage::Done => {}
        }
    }
}

const _: () = assert!(
    mem::size_of::<Call<'static, 'static, 'static, 'static, 'static, 0, method::channel::Open>>()
        == 11 * mem::size_of::<usize>()
);
