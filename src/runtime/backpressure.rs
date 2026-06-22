use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use dope::WakeRef;
use dope::fiber::Holding;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::transport::Transport;

use crate::Error;
use crate::protocol::Session;

pub struct WaitUnblocked<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding: Holding<'d, Connector<0, Session, S, E>>,
}

impl<'d, S, E> WaitUnblocked<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(super) fn new(holding: Holding<'d, Connector<0, Session, S, E>>) -> Self {
        Self { holding }
    }
}

impl<'d, S, E> Future for WaitUnblocked<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut h = self.holding.hold();
        if let Some(err) = h.session().shared.fatal.as_ref() {
            return Poll::Ready(Err(err.clone()));
        }
        if !h.session().shared.is_blocked() {
            return Poll::Ready(Ok(()));
        }
        h.as_mut()
            .session_mut()
            .shared
            .register_progress_waker(WakeRef::verified(cx.waker()));
        Poll::Pending
    }
}

pub struct WaitFlowResumed<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding: Holding<'d, Connector<0, Session, S, E>>,
    channel: u16,
}

impl<'d, S, E> WaitFlowResumed<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(super) fn new(holding: Holding<'d, Connector<0, Session, S, E>>, channel: u16) -> Self {
        Self { holding, channel }
    }
}

impl<'d, S, E> Future for WaitFlowResumed<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut h = self.holding.hold();
        if let Some(err) = h.session().shared.fatal.as_ref() {
            return Poll::Ready(Err(err.clone()));
        }
        let Some(target) = h.session().shared.ready_conn else {
            h.as_mut()
                .session_mut()
                .shared
                .register_handshake_waker(WakeRef::verified(cx.waker()));
            return Poll::Pending;
        };
        let Some(slot) = h.as_mut().state_for(target) else {
            return Poll::Ready(Err(Error::Closed));
        };
        let active = if let Some(c) = slot.conn_state_mut().channel_mut(self.channel) {
            c.flow_active()
        } else {
            true
        };
        if active {
            Poll::Ready(Ok(()))
        } else {
            h.as_mut()
                .session_mut()
                .shared
                .register_progress_waker(WakeRef::verified(cx.waker()));
            Poll::Pending
        }
    }
}
