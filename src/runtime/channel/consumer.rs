use std::pin::Pin;
use std::task::{Poll, ready};

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::{Context, Fiber, Waker};
use dope_net::Transport;
use o3::collections::SlabKey;

use crate::content::properties::Delivery;
use crate::error::Error;
use crate::method::basic;
use crate::method::core::ClientMethod;
use crate::protocol::channels::messages::ChannelMessages;
use crate::runtime::handles;
use crate::runtime::handles::{AmqpHandle, Missing};

fn retry_or_close<'d>(
    conn: handles::ConnectorRef<'d>,
    waker: Waker<'d>,
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
        shared.try_register_recovery_waker(waker)
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
        Recv { consumer: self }
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

pub struct Recv<'r, 'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    consumer: &'r mut Consumer<'a, 'd, S, E>,
}

impl<'r, 'a, 'd, S, E> Fiber<'d> for Recv<'r, 'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<Option<Delivery<'d>>, Error>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let conn = me.consumer.handle.connector();
        let waker = unsafe { cx.waker_unchecked() };
        let auto_recovery = conn
            .session()
            .with_shared(|shared| shared.auto_recovery_enabled());
        let Some(target) = ready!(conn.poll_target(waker, Missing::Recover))? else {
            return Poll::Ready(Ok(None));
        };

        let polled = match conn.with_state(target, |state| {
            state.with_channel(me.consumer.channel, |channel| {
                channel.consumer_next(me.consumer.key, waker)
            })
        }) {
            Some(polled) => polled,
            None => return retry_or_close(conn, waker, auto_recovery, true),
        };
        match polled {
            Some(Ok(Poll::Ready(Some(delivery)))) => {
                Poll::Ready(Ok(Some(unsafe { delivery.assume_lifetime() })))
            }
            Some(Ok(Poll::Ready(None))) => retry_or_close(conn, waker, auto_recovery, false),
            Some(Ok(Poll::Pending)) => Poll::Pending,
            Some(Err(error)) => Poll::Ready(Err(error)),
            None => retry_or_close(conn, waker, auto_recovery, false),
        }
    }
}
