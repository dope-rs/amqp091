use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Poll, ready};

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::{Context, Fiber};
use dope_net::Transport;
use o3::collections::SlabKey;

use crate::error::Error;
use crate::protocol::ExpectMethod;
use crate::protocol::session::outbound::Outbound;
use crate::runtime::channel::extract::Extract;
use crate::runtime::handles::{AmqpHandle, Missing};

pub struct MethodFuture<'a, 'd, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
    expect: ExpectMethod,
    frame: Option<Outbound>,
    op_id: Option<SlabKey>,
    error: Option<Error>,
    _r: PhantomData<fn() -> R>,
}

impl<'a, 'd, S, E, R> MethodFuture<'a, 'd, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    pub(crate) fn new(
        handle: AmqpHandle<'a, 'd, S, E>,
        channel: u16,
        expect: ExpectMethod,
        frame: Result<Outbound, Error>,
    ) -> Self {
        let (frame, error) = match frame {
            Ok(frame) => (Some(frame), None),
            Err(error) => (None, Some(error)),
        };
        Self {
            handle,
            channel,
            expect,
            frame,
            op_id: None,
            error,
            _r: PhantomData,
        }
    }
}

impl<'a, 'd, S, E, R> Fiber<'d> for MethodFuture<'a, 'd, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    type Output = Result<R, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let me = &mut *self;
        if let Some(error) = me.error.take() {
            return Poll::Ready(Err(error));
        }
        let conn = me.handle.connector();
        let waker = unsafe { cx.waker_unchecked() };

        if let Some(op_id) = me.op_id {
            let outcome = match conn
                .session()
                .with_channel(me.channel, |channel| channel.poll_op(op_id, waker))
            {
                None => return Poll::Ready(Err(Error::Closed)),
                Some(outcome) => outcome,
            };
            if let Some(outcome) = outcome {
                me.op_id = None;
                let outcome = outcome.and_then(R::extract);
                conn.session()
                    .with_shared(|shared| shared.collect_channel(me.channel));
                return Poll::Ready(outcome);
            }
            return Poll::Pending;
        }

        let Some(target) = ready!(conn.poll_target(waker, Missing::Wait))? else {
            unreachable!()
        };

        if let Some(frame) = me.frame.take() {
            let setup = conn.with_state(target, |state| {
                let Some(queued) = state.with_channel(me.channel, |channel| {
                    let (op_id, send) = channel.enqueue_op(me.expect, frame)?;
                    channel.poll_op(op_id, waker);
                    Ok::<_, Error>((op_id, send))
                }) else {
                    return Err(Error::Protocol("channel out of range"));
                };
                let (op_id, send) = queued?;
                if let Some(frame) = send
                    && state.try_enqueue(frame).is_err()
                {
                    state.with_channel(me.channel, |channel| channel.cancel_op(op_id));
                    return Err(Error::Other("egress queue full".into()));
                }
                Ok(op_id)
            });
            let op_id = match setup {
                None => return Poll::Ready(Err(Error::Closed)),
                Some(Err(error)) => return Poll::Ready(Err(error)),
                Some(Ok(op_id)) => op_id,
            };
            me.op_id = Some(op_id);
            return Poll::Pending;
        }

        Poll::Ready(Err(Error::Other(
            "method future polled without frame or operation".into(),
        )))
    }
}

impl<S, E, R> Drop for MethodFuture<'_, '_, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    fn drop(&mut self) {
        let Some(op_id) = self.op_id.take() else {
            return;
        };
        self.handle.connector().session().with_shared(|shared| {
            shared.with_channel_mut(self.channel, |channel| {
                channel.detach_op(op_id);
            });
            shared.collect_channel(self.channel);
        });
    }
}
