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
use crate::content::Returned;
use crate::protocol::Session;

pub struct NextReturn<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding: Holding<'d, Connector<0, Session, S, E>>,
    channel: u16,
}

impl<'d, S, E> NextReturn<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(super) fn new(holding: Holding<'d, Connector<0, Session, S, E>>, channel: u16) -> Self {
        Self { holding, channel }
    }
}

impl<'d, S, E> Future for NextReturn<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<Option<Returned>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut h = self.holding.hold();
        if let Some(err) = h.session().shared.fatal.as_ref() {
            return Poll::Ready(Err(err.clone()));
        }
        let waker = WakeRef::verified(cx.waker());
        let auto_recovery = h.session().shared.auto_recovery_enabled();
        let Some(target) = h.session().shared.ready_conn else {
            if auto_recovery {
                let shared = &mut h.as_mut().session_mut().shared;
                shared.register_handshake_waker(waker);
                return Poll::Pending;
            }
            return Poll::Ready(Ok(None));
        };
        let Some(slot) = h.as_mut().state_for(target) else {
            return Poll::Ready(Ok(None));
        };
        let state = slot.conn_state_mut();
        let Some(ch) = state.channel_mut(self.channel) else {
            return Poll::Ready(Ok(None));
        };
        match ch.next_return(waker) {
            Some(r) => Poll::Ready(Ok(Some(r))),
            None => Poll::Pending,
        }
    }
}
