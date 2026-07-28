use std::pin::Pin;
use std::task::Poll;

use dope::driver::ready::CompletionWaker;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::abi::Fiber;
use dope_fiber::raw::task::{CompletionRegistrar, Context};
use dope_net::Transport;

use crate::error::Error;
use crate::protocol::PublishConfirm;
use crate::runtime::handles::AmqpHandle;

pub struct PublishConfirmFuture<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
    delivery_tag: u64,
    early: Option<Error>,
}

// SAFETY: a live delivery tag owns the confirm slot. Every ready/error path
// retires or detaches that slot, and Drop detaches it on cancellation.
unsafe impl<'a, 'd, S, E> CompletionRegistrar<'d> for &mut PublishConfirmFuture<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Poll<Result<PublishConfirm, Error>>;

    #[inline(always)]
    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let conn = self.handle.connector();
        let outcome = match conn.session().with_channel(self.channel, |channel| {
            channel.poll_publish(self.delivery_tag, wake)
        }) {
            None => return Poll::Ready(Err(Error::Closed)),
            Some(outcome) => outcome,
        };
        match outcome {
            Ok(Some(outcome)) => {
                self.delivery_tag = 0;
                conn.session()
                    .with_shared(|shared| shared.collect_channel(self.channel));
                Poll::Ready(Ok(outcome))
            }
            Ok(None) => Poll::Pending,
            Err(error) => {
                conn.session().with_shared(|shared| {
                    shared.with_channel_mut(self.channel, |channel| {
                        channel.detach_publish(self.delivery_tag);
                    });
                    shared.collect_channel(self.channel);
                });
                self.delivery_tag = 0;
                Poll::Ready(Err(error))
            }
        }
    }
}

impl<'a, 'd, S, E> PublishConfirmFuture<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn confirmed(
        handle: AmqpHandle<'a, 'd, S, E>,
        channel: u16,
        delivery_tag: u64,
    ) -> Self {
        Self {
            handle,
            channel,
            delivery_tag,
            early: None,
        }
    }

    pub(in crate::runtime) fn failed(
        handle: AmqpHandle<'a, 'd, S, E>,
        channel: u16,
        err: Error,
    ) -> Self {
        Self {
            handle,
            channel,
            delivery_tag: 0,
            early: Some(err),
        }
    }
}

impl<'a, 'd, S, E> Fiber<'d> for PublishConfirmFuture<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<PublishConfirm, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let me = &mut *self;
        if let Some(err) = me.early.take() {
            return Poll::Ready(Err(err));
        }
        cx.as_ref().register_completion(me)
    }
}

impl<S, E> Drop for PublishConfirmFuture<'_, '_, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(&mut self) {
        if self.delivery_tag == 0 {
            return;
        }
        self.handle.connector().session().with_shared(|shared| {
            shared.with_channel_mut(self.channel, |channel| {
                channel.detach_publish(self.delivery_tag);
            });
            shared.collect_channel(self.channel);
        });
    }
}
