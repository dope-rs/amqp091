use std::pin::Pin;
use std::task::{Poll, ready};

use dope::driver::ready::{CompletionKey, CompletionWaker};
use dope::driver::token::Token;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::abi::Fiber;
use dope_fiber::raw::task::{CompletionRegistrar, Context};
use dope_fiber::raw::wait::Waiter;
use dope_net::Transport;
use pin_project::{pin_project, pinned_drop};

use crate::content::properties::Returned;
use crate::error::Error;
use crate::protocol::channels::messages::ChannelMessages;
use crate::runtime::handles::{AmqpHandle, Missing};

#[pin_project(PinnedDrop)]
pub struct NextReturn<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
    #[pin]
    handshake: Waiter<'d>,
    registration: Option<CompletionKey<'d>>,
}

struct PollReturn<'poll, 'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    owner: Pin<&'poll mut NextReturn<'a, 'd, S, E>>,
    target: Token,
}

// SAFETY: registration records the exact retained handle before next_return
// can store it. Pinned Drop clears that key before task unbinding.
unsafe impl<'poll, 'a, 'd, S, E> CompletionRegistrar<'d> for PollReturn<'poll, 'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Poll<Result<Option<Returned<'d>>, Error>>;

    #[inline(always)]
    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let key = wake.key();
        let this = self.owner.project();
        let conn = this.handle.connector();
        let returned = match conn.with_state(self.target, |state| {
            state.with_channel(*this.channel, |channel| {
                if let Some(previous) = this.registration.take() {
                    channel.clear_return_waker(previous);
                }
                *this.registration = Some(key);
                let returned = channel.next_return(wake);
                if !matches!(&returned, Ok(None)) {
                    *this.registration = None;
                }
                returned
            })
        }) {
            Some(Some(returned)) => returned,
            None | Some(None) => return Poll::Ready(Ok(None)),
        };
        match returned {
            Ok(Some(returned)) => Poll::Ready(Ok(Some(unsafe { returned.assume_lifetime() }))),
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl<'a, 'd, S, E> NextReturn<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(in crate::runtime) fn new(handle: AmqpHandle<'a, 'd, S, E>, channel: u16) -> Self {
        Self {
            handle,
            channel,
            handshake: Waiter::new(),
            registration: None,
        }
    }
}

impl<'a, 'd, S, E> Fiber<'d> for NextReturn<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<Option<Returned<'d>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let target = {
            let this = self.as_mut().project();
            let conn = this.handle.connector();
            let Some(target) =
                ready!(conn.poll_target(this.handshake.as_ref(), cx.as_ref(), Missing::Recover,))?
            else {
                return Poll::Ready(Ok(None));
            };
            target
        };
        cx.as_ref().register_completion(PollReturn {
            owner: self,
            target,
        })
    }
}

#[pinned_drop]
impl<S, E> PinnedDrop for NextReturn<'_, '_, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let Some(key) = this.registration.take() else {
            return;
        };
        this.handle.connector().session().with_shared(|shared| {
            shared.with_channel_mut(*this.channel, |channel| {
                channel.clear_return_waker(key);
            });
        });
    }
}
