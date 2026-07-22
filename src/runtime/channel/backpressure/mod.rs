use std::pin::Pin;
use std::task::{Poll, ready};

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::{Context, Fiber};
use dope_net::Transport;

use crate::error::Error;
use crate::runtime::handles::{AmqpHandle, Missing};

pub struct WaitUnblocked<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
}

impl<'a, 'd, S, E> WaitUnblocked<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn new(handle: AmqpHandle<'a, 'd, S, E>) -> Self {
        Self { handle }
    }
}

impl<'a, 'd, S, E> Fiber<'d> for WaitUnblocked<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let waker = unsafe { cx.waker_unchecked() };
        self.handle.connector().session().with_shared(|shared| {
            if let Some(error) = shared.fatal.as_ref() {
                return Poll::Ready(Err(error.clone()));
            }
            if !shared.is_blocked() {
                return Poll::Ready(Ok(()));
            }
            if shared.try_register_progress_waker(waker) {
                Poll::Pending
            } else {
                Poll::Ready(Err(Error::WaiterCapacity))
            }
        })
    }
}

pub struct WaitFlowResumed<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
}

impl<'a, 'd, S, E> WaitFlowResumed<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn new(handle: AmqpHandle<'a, 'd, S, E>, channel: u16) -> Self {
        Self { handle, channel }
    }
}

impl<'a, 'd, S, E> Fiber<'d> for WaitFlowResumed<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let conn = self.handle.connector();
        let waker = unsafe { cx.waker_unchecked() };
        let Some(target) = ready!(conn.poll_target(waker, Missing::Wait))? else {
            unreachable!()
        };
        let active = match conn.with_state(target, |state| {
            state.with_channel(self.channel, |channel| channel.flow_active())
        }) {
            None => return Poll::Ready(Err(Error::Closed)),
            Some(active) => active.unwrap_or(true),
        };
        if active {
            Poll::Ready(Ok(()))
        } else {
            if conn
                .session()
                .with_shared(|shared| shared.try_register_progress_waker(waker))
            {
                Poll::Pending
            } else {
                Poll::Ready(Err(Error::WaiterCapacity))
            }
        }
    }
}
