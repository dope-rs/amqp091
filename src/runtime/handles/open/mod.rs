use super::*;
use crate::method::channel::Open;

pub struct OpenChannel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
    channel: u16,
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

    fn poll(mut self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        if let Some(error) = self.error.take() {
            return Poll::Ready(Err(error));
        }
        match Pin::new(&mut self.future).poll(cx) {
            Poll::Ready(Ok(())) => {
                self.reserved = false;
                Poll::Ready(Ok(Channel {
                    handle: self.handle,
                    channel: self.channel,
                }))
            }
            Poll::Ready(Err(error)) => {
                self.release();
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, E> OpenChannel<'_, '_, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn release(&mut self) {
        if self.reserved {
            self.handle
                .connector()
                .session()
                .with_shared(|shared| shared.release_channel(self.channel));
            self.reserved = false;
        }
    }
}

impl<S, E> Drop for OpenChannel<'_, '_, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(&mut self) {
        self.release();
    }
}
