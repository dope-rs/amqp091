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

use crate::content::properties::Delivery;
use crate::error::Error;
use crate::method::basic;
use crate::method::core::ClientMethod;
use crate::protocol::channels::messages::ChannelMessages;
use crate::runtime::handles;
use crate::runtime::handles::{AmqpHandle, Missing};

fn retry_or_close<'d>(
    conn: handles::ConnectorRef<'d>,
    progress: Pin<&Waiter<'d>>,
    handshake: Pin<&Waiter<'d>>,
    context: Pin<&Context<'_, 'd>>,
    auto_recovery: bool,
    clear_target: bool,
) -> Poll<Result<Option<Delivery<'d>>, Error>> {
    if !auto_recovery {
        return Poll::Ready(Ok(None));
    }
    let registered = conn.session().with_shared(|shared| {
        if clear_target {
            shared.clear_dead_ready_conn();
        }
        shared.try_register_recovery_waker(progress, handshake, context)
    });
    if registered {
        Poll::Pending
    } else {
        Poll::Ready(Err(Error::WaiterCapacity))
    }
}

pub struct Consumer<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
    key: SlabKey,
    tag: String,
}

impl<'a, 'd, S, E> Consumer<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn new(
        handle: AmqpHandle<'a, 'd, S, E>,
        channel: u16,
        key: SlabKey,
        tag: String,
    ) -> Self {
        Self {
            handle,
            channel,
            key,
            tag,
        }
    }

    pub fn channel_id(&self) -> u16 {
        self.channel
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn recv(&mut self) -> Recv<'_, 'a, 'd, S, E> {
        Recv {
            consumer: self,
            handshake: Waiter::new(),
            progress: Waiter::new(),
        }
    }
}

impl<S, E> Drop for Consumer<'_, '_, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(&mut self) {
        let conn = self.handle.connector();
        let (target, retiring) = conn.session().with_shared(|shared| {
            let retiring = shared
                .with_channel_mut(self.channel, |channel| {
                    channel.detach_consumer(self.key);
                    channel.is_retiring()
                })
                .unwrap_or(true);
            shared
                .topology_mut()
                .remove_consumer(self.channel, &self.tag);
            shared.collect_channel(self.channel);
            (shared.ready_conn, retiring)
        });
        if retiring {
            return;
        }
        let Some(target) = target else {
            return;
        };
        let Ok(frame) = self.handle.encode(|out| {
            basic::CancelRef {
                consumer_tag: &self.tag,
                no_wait: true,
            }
            .write_frame(out, self.channel);
        }) else {
            return;
        };
        conn.with_state(target, |state| state.try_enqueue(frame));
    }
}

#[pin_project(PinnedDrop)]
pub struct Recv<'r, 'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    consumer: &'r mut Consumer<'a, 'd, S, E>,
    #[pin]
    handshake: Waiter<'d>,
    #[pin]
    progress: Waiter<'d>,
}

struct PollConsumer<'poll, 'r, 'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    owner: Pin<&'poll mut Recv<'r, 'a, 'd, S, E>>,
    target: Token,
}

// SAFETY: pinned Recv exclusively owns this consumer registration. Its pinned
// Drop clears the exact slot before the task context can be unbound.
unsafe impl<'poll, 'r, 'a, 'd, S, E> CompletionRegistrar<'d>
    for PollConsumer<'poll, 'r, 'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Option<Option<Result<Poll<Option<Delivery<'static>>>, Error>>>;

    #[inline(always)]
    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let this = self.owner.project();
        let consumer = &mut **this.consumer;
        consumer
            .handle
            .connector()
            .with_state(self.target, |state| {
                state.with_channel(consumer.channel, |channel| {
                    channel.consumer_next(consumer.key, wake)
                })
            })
    }
}

impl<'r, 'a, 'd, S, E> Fiber<'d> for Recv<'r, 'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<Option<Delivery<'d>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let (conn, auto_recovery, target) = {
            let this = self.as_mut().project();
            let consumer = &mut **this.consumer;
            let conn = consumer.handle.connector();
            let auto_recovery = conn
                .session()
                .with_shared(|shared| shared.auto_recovery_enabled());
            let Some(target) =
                ready!(conn.poll_target(this.handshake.as_ref(), cx.as_ref(), Missing::Recover,))?
            else {
                return Poll::Ready(Ok(None));
            };
            (conn, auto_recovery, target)
        };

        let polled = match cx.as_ref().register_completion(PollConsumer {
            owner: self.as_mut(),
            target,
        }) {
            Some(polled) => polled,
            None => {
                let this = self.as_ref().project_ref();
                return retry_or_close(
                    conn,
                    this.progress.as_ref(),
                    this.handshake.as_ref(),
                    cx.as_ref(),
                    auto_recovery,
                    true,
                );
            }
        };
        let this = self.as_ref().project_ref();
        match polled {
            Some(Ok(Poll::Ready(Some(delivery)))) => {
                Poll::Ready(Ok(Some(unsafe { delivery.assume_lifetime() })))
            }
            Some(Ok(Poll::Ready(None))) => retry_or_close(
                conn,
                this.progress.as_ref(),
                this.handshake.as_ref(),
                cx.as_ref(),
                auto_recovery,
                false,
            ),
            Some(Ok(Poll::Pending)) => Poll::Pending,
            Some(Err(error)) => Poll::Ready(Err(error)),
            None => retry_or_close(
                conn,
                this.progress.as_ref(),
                this.handshake.as_ref(),
                cx.as_ref(),
                auto_recovery,
                false,
            ),
        }
    }
}

#[pinned_drop]
impl<S, E> PinnedDrop for Recv<'_, '_, '_, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let consumer = &mut **this.consumer;
        consumer.handle.connector().session().with_shared(|shared| {
            shared.with_channel_mut(consumer.channel, |channel| {
                channel.clear_consumer_waker(consumer.key);
            });
        });
    }
}
