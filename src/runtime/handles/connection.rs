use super::*;
use dope_fiber::wait::WaitFn;

pub struct Connection<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
}

impl<'a, 'd, S, E> Clone for Connection<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, 'd, S, E> Copy for Connection<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
}

impl<'a, 'd, S, E> Connection<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn from_handle(handle: AmqpHandle<'a, 'd, S, E>) -> Self {
        Self { handle }
    }

    pub fn handle(&self) -> AmqpHandle<'a, 'd, S, E> {
        self.handle
    }

    pub fn ready_generation(&self) -> u64 {
        self.handle
            .connector()
            .session()
            .with_shared(|shared| shared.ready_generation())
    }

    pub fn handshake(&self) -> Handshake<'a, 'd, S, E> {
        Handshake::new(self.handle)
    }

    pub fn wait_unblocked(&self) -> WaitUnblocked<'a, 'd, S, E> {
        WaitUnblocked::new(self.handle)
    }

    pub fn is_blocked(&self) -> bool {
        self.handle
            .connector()
            .session()
            .with_shared(|shared| shared.is_blocked())
    }

    pub fn is_ready(&self) -> bool {
        self.handle
            .connector()
            .session()
            .with_shared(|shared| shared.ready_conn.is_some())
    }

    pub fn is_failed(&self) -> bool {
        self.handle
            .connector()
            .session()
            .with_shared(|shared| shared.fatal.is_some())
    }

    pub(crate) fn channel(&self, id: u16) -> Channel<'a, 'd, S, E> {
        Channel {
            handle: self.handle,
            channel: id,
        }
    }

    pub fn open(&self, id: u16) -> OpenChannel<'a, 'd, S, E> {
        let reservation = self
            .handle
            .connector()
            .session()
            .with_shared(|shared| shared.reserve_channel(id));
        OpenChannel::new(self.handle, id, reservation)
    }

    pub fn open_auto_channel<'b>(
        &'b self,
    ) -> impl Fiber<'d, Output = Result<Channel<'a, 'd, S, E>, Error>> + 'b {
        dope_fiber::fiber!('d => async move {
            let id = self.handle.connector()
                .session()
                .with_shared(|shared| {
                    let channel_max = shared.negotiated.channel_max;
                    shared.next_unused_channel(channel_max)
                })
                .ok_or_else(|| Error::Other("no free channel id available".into()))?;
            OpenChannel::new(self.handle, id, Ok(())).await
        })
    }

    pub fn close_now(&self, reply_code: u16, reply_text: &str) {
        let conn = self.handle.connector();
        let target = conn.session().with_shared(|shared| {
            shared.mark_intentional_close();
            shared.ready_conn
        });
        if let Some(target) = target
            && let Ok(frame) = self.handle.port.encode(|out| {
                Close {
                    reply_code,
                    reply_text: reply_text.into(),
                    class_id: 0,
                    method_id: 0,
                }
                .write_frame(out, 0);
            })
        {
            conn.with_state(target, |state| state.try_enqueue(frame));
        }
    }

    pub fn close<'b>(
        &'b self,
        reply_code: u16,
        reply_text: &'b str,
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        dope_fiber::fiber!('d => async move {
            let handle = self.handle;
            self.close_now(reply_code, reply_text);
            WaitFn::new(move |cx, waiter| {
                handle.connector().session().with_shared(|shared| {
                    if shared.ready_conn.is_none() {
                        return Poll::Ready(Ok(()));
                    }
                    if let Some(err) = shared.fatal.as_ref() {
                        return Poll::Ready(Err(err.clone()));
                    }
                    if shared.try_register_closed_waker(waiter, cx.as_ref()) {
                        Poll::Pending
                    } else {
                        Poll::Ready(Err(Error::WaiterCapacity))
                    }
                })
            })
            .await
        })
    }
}
