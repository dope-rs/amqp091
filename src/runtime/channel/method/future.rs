use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Poll, ready};

use dope::driver::ready::CompletionWaker;
use dope::driver::token::Token;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::abi::Fiber;
use dope_fiber::raw::task::{CompletionRegistrar, Context};
use dope_fiber::raw::wait::Waiter;
use dope_net::Transport;
use o3::collections::SlabKey;
use pin_project::{pin_project, pinned_drop};

use crate::error::Error;
use crate::protocol::ExpectMethod;
use crate::protocol::session::outbound::Outbound;
use crate::runtime::channel::extract::Extract;
use crate::runtime::handles::{AmqpHandle, Missing};

#[pin_project(PinnedDrop)]
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
    frame: Option<Outbound<'d>>,
    op_id: Option<SlabKey>,
    error: Option<Error>,
    #[pin]
    handshake: Waiter<'d>,
    _r: PhantomData<fn() -> R>,
}

struct PollMethod<'poll, 'a, 'd, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    owner: Pin<&'poll mut MethodFuture<'a, 'd, S, E, R>>,
    op_id: SlabKey,
}

// SAFETY: while op_id is present, pinned Drop detaches the exact operation.
// poll_op either retains the handle and returns Pending or retires the slot.
unsafe impl<'poll, 'a, 'd, S, E, R> CompletionRegistrar<'d> for PollMethod<'poll, 'a, 'd, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    type Output = Poll<Result<R, Error>>;

    #[inline(always)]
    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let this = self.owner.project();
        let conn = this.handle.connector();
        let outcome = match conn
            .session()
            .with_channel(*this.channel, |channel| channel.poll_op(self.op_id, wake))
        {
            None => return Poll::Ready(Err(Error::Closed)),
            Some(outcome) => outcome,
        };
        let Some(outcome) = outcome else {
            return Poll::Pending;
        };
        *this.op_id = None;
        let outcome = outcome.and_then(R::extract);
        conn.session()
            .with_shared(|shared| shared.collect_channel(*this.channel));
        Poll::Ready(outcome)
    }
}

struct StartMethod<'poll, 'a, 'd, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    owner: Pin<&'poll mut MethodFuture<'a, 'd, S, E, R>>,
    target: Token,
    frame: Outbound<'d>,
}

// SAFETY: op_id is recorded in the pinned owner before the handle is retained.
// Every subsequent error cancels it, and pinned Drop detaches pending work.
unsafe impl<'poll, 'a, 'd, S, E, R> CompletionRegistrar<'d> for StartMethod<'poll, 'a, 'd, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    type Output = Poll<Result<R, Error>>;

    #[inline(always)]
    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let this = self.owner.project();
        let conn = this.handle.connector();
        let setup = conn.with_state(self.target, |state| {
            let Some(queued) = state.with_channel(*this.channel, |channel| {
                let (op_id, send) = channel.enqueue_op(*this.expect, self.frame)?;
                *this.op_id = Some(op_id);
                channel.poll_op(op_id, wake);
                Ok::<_, Error>((op_id, send))
            }) else {
                return Err(Error::Protocol("channel out of range"));
            };
            let (op_id, send) = queued?;
            if let Some(frame) = send
                && state.try_enqueue(frame).is_err()
            {
                state.with_channel(*this.channel, |channel| channel.cancel_op(op_id));
                *this.op_id = None;
                return Err(Error::Other("egress queue full".into()));
            }
            Ok(())
        });
        match setup {
            None => Poll::Ready(Err(Error::Closed)),
            Some(Err(error)) => Poll::Ready(Err(error)),
            Some(Ok(())) => Poll::Pending,
        }
    }
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
        frame: Result<Outbound<'d>, Error>,
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
            handshake: Waiter::new(),
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
        {
            let this = self.as_mut().project();
            if let Some(error) = this.error.take() {
                return Poll::Ready(Err(error));
            }
            if let Some(op_id) = *this.op_id {
                return cx
                    .as_ref()
                    .register_completion(PollMethod { owner: self, op_id });
            }
        }

        let target = {
            let this = self.as_mut().project();
            let conn = this.handle.connector();
            let Some(target) =
                ready!(conn.poll_target(this.handshake.as_ref(), cx.as_ref(), Missing::Wait,))?
            else {
                unreachable!()
            };
            target
        };

        if let Some(frame) = self.as_mut().project().frame.take() {
            return cx.as_ref().register_completion(StartMethod {
                owner: self,
                target,
                frame,
            });
        }

        Poll::Ready(Err(Error::Other(
            "method future polled without frame or operation".into(),
        )))
    }
}

#[pinned_drop]
impl<S, E, R> PinnedDrop for MethodFuture<'_, '_, S, E, R>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
    R: Extract,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let Some(op_id) = this.op_id.take() else {
            return;
        };
        this.handle.connector().session().with_shared(|shared| {
            shared.with_channel_mut(*this.channel, |channel| {
                channel.detach_op(op_id);
            });
            shared.collect_channel(*this.channel);
        });
    }
}
