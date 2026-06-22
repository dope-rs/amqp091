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
use crate::protocol::{PublishConfirm, Session};

pub struct PublishConfirmFuture<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding: Holding<'d, Connector<0, Session, S, E>>,
    channel: u16,
    delivery_tag: u64,
    early: Option<Error>,
}

impl<'d, S, E> PublishConfirmFuture<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(super) fn confirmed(
        holding: Holding<'d, Connector<0, Session, S, E>>,
        channel: u16,
        delivery_tag: u64,
    ) -> Self {
        Self {
            holding,
            channel,
            delivery_tag,
            early: None,
        }
    }

    pub(super) fn failed(
        holding: Holding<'d, Connector<0, Session, S, E>>,
        channel: u16,
        err: Error,
    ) -> Self {
        Self {
            holding,
            channel,
            delivery_tag: 0,
            early: Some(err),
        }
    }
}

impl<'d, S, E> Future for PublishConfirmFuture<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<PublishConfirm, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        if let Some(err) = me.early.take() {
            return Poll::Ready(Err(err));
        }
        let mut h = me.holding.hold();
        if let Some(err) = h.session().shared.fatal.as_ref() {
            return Poll::Ready(Err(err.clone()));
        }
        let target = match h.session().shared.ready_conn {
            Some(t) => t,
            None => return Poll::Ready(Err(Error::Closed)),
        };
        let slot = match h.as_mut().state_for(target) {
            Some(s) => s,
            None => return Poll::Ready(Err(Error::Closed)),
        };
        let state = slot.conn_state_mut();
        let ch = match state.channel_mut(me.channel) {
            Some(c) => c,
            None => return Poll::Ready(Err(Error::Protocol("channel out of range"))),
        };
        match ch.poll_publish(me.delivery_tag, WakeRef::verified(cx.waker()))? {
            Some(outcome) => Poll::Ready(Ok(outcome)),
            None => Poll::Pending,
        }
    }
}
