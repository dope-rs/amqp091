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
use crate::content::Delivery;
use crate::protocol::{ConsumerPoll, Session};

pub struct Consumer<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    holding: Holding<'d, Connector<0, Session, S, E>>,
    channel: u16,
    tag: String,
}

impl<'d, S, E> Consumer<'d, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn new(
        holding: Holding<'d, Connector<0, Session, S, E>>,
        channel: u16,
        tag: String,
    ) -> Self {
        Self {
            holding,
            channel,
            tag,
        }
    }

    pub fn channel_id(&self) -> u16 {
        self.channel
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn recv(&mut self) -> Recv<'_, 'd, S, E> {
        Recv { consumer: self }
    }
}

pub struct Recv<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    consumer: &'a mut Consumer<'d, S, E>,
}

impl<'a, 'd, S, E> Future for Recv<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<Option<Delivery>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let mut h = me.consumer.holding.hold();

        if let Some(err) = h.session().shared.fatal.as_ref() {
            return Poll::Ready(Err(err.clone()));
        }
        let waker = WakeRef::verified(cx.waker());
        let auto_recovery = h.session().shared.auto_recovery_enabled();

        let target = match h.session().shared.ready_conn {
            Some(t) => t,
            None => {
                if auto_recovery {
                    let shared = &mut h.as_mut().session_mut().shared;
                    shared.register_handshake_waker(waker);
                    shared.register_progress_waker(waker);
                    return Poll::Pending;
                } else {
                    return Poll::Ready(Ok(None));
                }
            }
        };

        let slot = match h.as_mut().state_for(target) {
            Some(s) => s,
            None => {
                if auto_recovery {
                    let shared = &mut h.as_mut().session_mut().shared;
                    shared.clear_dead_ready_conn();
                    shared.register_handshake_waker(waker);
                    shared.register_progress_waker(waker);
                    return Poll::Pending;
                }
                return Poll::Ready(Ok(None));
            }
        };
        let state = slot.conn_state_mut();
        match state.channel_mut(me.consumer.channel) {
            Some(ch) => match ch.consumer_next(&me.consumer.tag, waker) {
                ConsumerPoll::Ready(d) => Poll::Ready(Ok(Some(*d))),
                ConsumerPoll::Canceled => {
                    if auto_recovery {
                        let shared = &mut h.as_mut().session_mut().shared;
                        shared.register_progress_waker(waker);
                        shared.register_handshake_waker(waker);
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(None))
                    }
                }
                ConsumerPoll::Pending => Poll::Pending,
            },
            None => {
                if auto_recovery {
                    let shared = &mut h.as_mut().session_mut().shared;
                    shared.register_progress_waker(waker);
                    shared.register_handshake_waker(waker);
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(None))
                }
            }
        }
    }
}
