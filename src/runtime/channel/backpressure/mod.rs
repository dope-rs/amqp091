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
pub struct WaitUnblocked<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'a, 'd, S, E> WaitUnblocked<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn new(handle: AmqpHandle<'a, 'd, S, E>) -> Self {
        Self {
            handle,
            waiter: Waiter::new(),
        }
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
        let this = self.project();
        this.handle.connector().session().with_shared(|shared| {
            if let Some(error) = shared.fatal.as_ref() {
                return Poll::Ready(Err(error.clone()));
            }
            if !shared.is_blocked() {
                return Poll::Ready(Ok(()));
            }
            if shared.try_register_progress_waker(this.waiter.as_ref(), cx.as_ref()) {
                Poll::Pending
            } else {
                Poll::Ready(Err(Error::WaiterCapacity))
            }
        })
    }
}

#[pin_project]
pub struct WaitFlowResumed<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
    #[pin]
    handshake: Waiter<'d>,
    #[pin]
    progress: Waiter<'d>,
}

impl<'a, 'd, S, E> WaitFlowResumed<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn new(handle: AmqpHandle<'a, 'd, S, E>, channel: u16) -> Self {
        Self {
            handle,
            channel,
            handshake: Waiter::new(),
            progress: Waiter::new(),
        }
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
        let this = self.project();
        let conn = this.handle.connector();
        let Some(target) =
            ready!(conn.poll_target(this.handshake.as_ref(), cx.as_ref(), Missing::Wait,))?
        else {
            unreachable!()
        };
        let active = match conn.with_state(target, |state| {
            state.with_channel(*this.channel, |channel| channel.flow_active())
        }) {
            None => return Poll::Ready(Err(Error::Closed)),
            Some(active) => active.unwrap_or(true),
        };
        if active {
            Poll::Ready(Ok(()))
        } else {
            if conn.session().with_shared(|shared| {
                shared.try_register_progress_waker(this.progress.as_ref(), cx.as_ref())
            }) {
                Poll::Pending
            } else {
                Poll::Ready(Err(Error::WaiterCapacity))
            }
        }
    }
}
