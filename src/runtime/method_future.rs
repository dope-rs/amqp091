use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use dope::WakeRef;
use dope::fiber::Holding;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::transport::Transport;
use o3::buffer::Shared;

use crate::Error;
use crate::protocol::{ExpectMethod, Session};
use crate::runtime::extract::Extract;

pub struct MethodFuture<'d, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    holding: Holding<'d, Connector<0, Session, S, E>>,
    channel: u16,
    expect: ExpectMethod,
    frame: Option<Shared>,
    op_id: Option<u64>,
    _r: PhantomData<fn() -> R>,
}

impl<'d, S, E, R> MethodFuture<'d, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    pub fn new(
        holding: Holding<'d, Connector<0, Session, S, E>>,
        channel: u16,
        expect: ExpectMethod,
        frame: Shared,
    ) -> Self {
        Self {
            holding,
            channel,
            expect,
            frame: Some(frame),
            op_id: None,
            _r: PhantomData,
        }
    }
}

impl<'d, S, E, R> Future for MethodFuture<'d, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    type Output = Result<R, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        let mut h = me.holding.hold();

        if let Some(err) = h.session().shared.fatal.as_ref() {
            return Poll::Ready(Err(err.clone()));
        }
        let waker = WakeRef::verified(cx.waker());
        let target = match h.session().shared.ready_conn {
            Some(t) => t,
            None => {
                h.as_mut()
                    .session_mut()
                    .shared
                    .register_handshake_waker(waker);
                return Poll::Pending;
            }
        };

        if let Some(frame) = me.frame.take() {
            let slot = match h.as_mut().state_for(target) {
                Some(s) => s,
                None => return Poll::Ready(Err(Error::Closed)),
            };
            let state = slot.conn_state_mut();
            let ch = match state.channel_mut(me.channel) {
                Some(c) => c,
                None => return Poll::Ready(Err(Error::Protocol("channel out of range"))),
            };
            let (op_id, send_now) = ch.enqueue_op(me.expect, frame.clone());
            me.op_id = Some(op_id);
            ch.poll_op(op_id, waker);
            if send_now && !slot.enqueue(frame) {
                if let Some(ch2) = slot.conn_state_mut().channel_mut(me.channel) {
                    ch2.cancel_op(op_id);
                }
                me.op_id = None;
                return Poll::Ready(Err(Error::Other("egress queue full".into())));
            }
            h.as_mut().request_flush(target);
            return Poll::Pending;
        }

        let Some(op_id) = me.op_id else {
            return Poll::Ready(Err(Error::Other("method future polled without op".into())));
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
        if let Some(outcome) = ch.poll_op(op_id, waker) {
            me.op_id = None;
            return Poll::Ready(outcome.and_then(R::extract));
        }
        Poll::Pending
    }
}
