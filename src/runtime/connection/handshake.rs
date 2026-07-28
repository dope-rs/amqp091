use std::pin::Pin;
use std::task::{Poll, ready};

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::abi::Fiber;
use dope_fiber::raw::task::Context;
use dope_fiber::raw::wait::Waiter;
use dope_net::Transport;
use pin_project::pin_project;

use crate::error::Error;
use crate::runtime::handles::{AmqpHandle, Missing};

#[pin_project]
pub struct Handshake<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'a, 'd, S, E> Handshake<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn new(handle: AmqpHandle<'a, 'd, S, E>) -> Self {
        Self {
            handle,
            waiter: Waiter::new(),
        }
    }
}

impl<'a, 'd, S, E> Fiber<'d> for Handshake<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        let conn = this.handle.connector();
        let Some(_) = ready!(conn.poll_target(this.waiter.as_ref(), cx.as_ref(), Missing::Wait))?
        else {
            unreachable!()
        };
        Poll::Ready(Ok(()))
    }
}
