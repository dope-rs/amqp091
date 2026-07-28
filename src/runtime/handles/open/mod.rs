use super::*;
use crate::method::channel::Open;
use pin_project::{pin_project, pinned_drop};

#[pin_project(PinnedDrop)]
pub struct OpenChannel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
    #[pin]
    future: MethodFuture<'a, 'd, S, E, ()>,
    error: Option<Error>,
    reserved: bool,
}

impl<'a, 'd, S, E> OpenChannel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(super) fn new(
        handle: AmqpHandle<'a, 'd, S, E>,
        channel: u16,
        reservation: Result<(), Error>,
    ) -> Self {
        let frame = handle.port.encode(|out| Open.write_frame(out, channel));
        let future =
            MethodFuture::<S, E, ()>::new(handle, channel, ExpectMethod::ChannelOpenOk, frame);
        let (error, reserved) = match reservation {
            Ok(()) => (None, true),
            Err(error) => (Some(error), false),
        };
        Self {
            handle,
            channel,
            future,
            error,
            reserved,
        }
    }
}

impl<'a, 'd, S, E> Fiber<'d> for OpenChannel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<Channel<'a, 'd, S, E>, Error>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        if let Some(error) = this.error.take() {
            return Poll::Ready(Err(error));
        }
        match this.future.poll(cx) {
            Poll::Ready(Ok(())) => {
                *this.reserved = false;
                Poll::Ready(Ok(Channel {
                    handle: *this.handle,
                    channel: *this.channel,
                }))
            }
            Poll::Ready(Err(error)) => {
                release_reservation(*this.handle, *this.channel, this.reserved);
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn release_reservation<S, E>(handle: AmqpHandle<'_, '_, S, E>, channel: u16, reserved: &mut bool)
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    if *reserved {
        handle
            .connector()
            .session()
            .with_shared(|shared| shared.release_channel(channel));
        *reserved = false;
    }
}

#[pinned_drop]
impl<S, E> PinnedDrop for OpenChannel<'_, '_, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        release_reservation(*this.handle, *this.channel, this.reserved);
    }
}
