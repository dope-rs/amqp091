use std::pin::Pin;
use std::task::{Poll, ready};

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::{Context, Fiber};
use dope_net::Transport;

use crate::error::Error;
use crate::runtime::handles::{AmqpHandle, Missing};

pub struct Handshake<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
}

impl<'a, 'd, S, E> Handshake<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn new(handle: AmqpHandle<'a, 'd, S, E>) -> Self {
        Self { handle }
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
        let conn = self.handle.connector();
        let Some(_) = ready!(conn.poll_target(unsafe { cx.waker_unchecked() }, Missing::Wait,))?
        else {
            unreachable!()
        };
        Poll::Ready(Ok(()))
    }
}
