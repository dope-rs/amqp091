use std::pin::Pin;
use std::task::Poll;

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::{Context, Fiber};
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
        let conn = me.handle.connector();
        let waker = unsafe { cx.waker_unchecked() };
        let outcome = match conn.session().with_channel(me.channel, |channel| {
            channel.poll_publish(me.delivery_tag, waker)
        }) {
            None => return Poll::Ready(Err(Error::Closed)),
            Some(outcome) => outcome,
        };
        match outcome {
            Ok(Some(outcome)) => {
                me.delivery_tag = 0;
                conn.session()
                    .with_shared(|shared| shared.collect_channel(me.channel));
                Poll::Ready(Ok(outcome))
            }
            Ok(None) => Poll::Pending,
            Err(error) => {
                me.delivery_tag = 0;
                conn.session()
                    .with_shared(|shared| shared.collect_channel(me.channel));
                Poll::Ready(Err(error))
            }
        }
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
