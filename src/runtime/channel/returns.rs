use std::pin::Pin;
use std::task::{Poll, ready};

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::{Context, Fiber};
use dope_net::Transport;

use crate::content::properties::Returned;
use crate::error::Error;
use crate::protocol::channels::messages::ChannelMessages;
use crate::runtime::handles::{AmqpHandle, Missing};

pub struct NextReturn<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
}

impl<'a, 'd, S, E> NextReturn<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn new(handle: AmqpHandle<'a, 'd, S, E>, channel: u16) -> Self {
        Self { handle, channel }
    }
}

impl<'a, 'd, S, E> Fiber<'d> for NextReturn<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<Option<Returned<'d>>, Error>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let conn = self.handle.connector();
        let waker = unsafe { cx.waker_unchecked() };
        let Some(target) = ready!(conn.poll_target(waker, Missing::Recover))? else {
            return Poll::Ready(Ok(None));
        };
        let returned = match conn.with_state(target, |state| {
            state.with_channel(self.channel, |channel| channel.next_return(waker))
        }) {
            Some(Some(returned)) => returned?,
            None | Some(None) => return Poll::Ready(Ok(None)),
        };
        match returned {
            Some(returned) => Poll::Ready(Ok(Some(unsafe { returned.assume_lifetime() }))),
            None => Poll::Pending,
        }
    }
}
