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

pub struct Handshake<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding: Holding<'d, Connector<0, Session, S, E>>,
}

impl<'d, S, E> Handshake<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn new(holding: Holding<'d, Connector<0, Session, S, E>>) -> Self {
        Self { holding }
    }
}

impl<'d, S, E> Future for Handshake<'d, S, E>
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
        if let Some(target) = h.session().shared.ready_conn {
            if h.as_mut().state_for(target).is_some() {
                return Poll::Ready(Ok(()));
            }
            h.as_mut().session_mut().shared.clear_dead_ready_conn();
        }
        h.as_mut()
            .session_mut()
            .shared
            .register_handshake_waker(WakeRef::verified(cx.waker()));
        Poll::Pending
    }
}
