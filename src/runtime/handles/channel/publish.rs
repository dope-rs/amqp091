use super::*;

pub trait PublishOperations<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn basic_publish(
        &self,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: &BasicProperties,
        body: &[u8],
    ) -> bool;
    fn basic_publish_confirm(
        &self,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: &BasicProperties,
        body: &[u8],
    ) -> PublishConfirmFuture<'a, 'd, S, E>;
}

impl<'a, 'd, S, E> PublishOperations<'a, 'd, S, E> for Channel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    fn basic_publish(
        &self,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: &BasicProperties,
        body: &[u8],
    ) -> bool {
        let conn = self.handle.connector();
        let target = conn.session().with_shared(|shared| {
            (shared.fatal.is_none() && !shared.is_blocked()).then_some(shared.ready_conn)
        });
        let Some(Some(target)) = target else {
            return false;
        };
        conn.with_state(target, |state| {
            if state
                .with_channel(self.channel, |channel| channel.flow_active())
                .is_some_and(|active| !active)
            {
                return false;
            }
            let Ok(frames) = self.build_publish_frames(
                exchange,
                routing_key,
                mandatory,
                properties,
                body,
                state.negotiated().frame_max,
            ) else {
                return false;
            };
            state.try_enqueue(frames).is_ok()
        })
        .unwrap_or(false)
    }

    fn basic_publish_confirm(
        &self,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: &BasicProperties,
        body: &[u8],
    ) -> PublishConfirmFuture<'a, 'd, S, E> {
        let conn = self.handle.connector();
        let state = conn.session().with_shared(|shared| {
            if let Some(err) = shared.fatal.as_ref() {
                return Err(err.clone());
            }
            if shared.is_blocked() {
                return Err(Error::ConnectionBlocked(
                    shared.blocked_reason().to_string(),
                ));
            }
            shared.ready_conn.ok_or(Error::Closed)
        });
        let target = match state {
            Ok(target) => target,
            Err(error) => {
                return PublishConfirmFuture::failed(self.handle, self.channel, error);
            }
        };
        let setup = conn.with_state(target, |state| {
            let tag = state
                .with_channel(self.channel, |channel| {
                    if !channel.flow_active() {
                        Err(Error::ChannelFlowPaused)
                    } else if !channel.confirm_enabled() {
                        Err(Error::Protocol(
                            "publisher confirms not enabled (call confirm_select first)",
                        ))
                    } else {
                        channel.reserve_publish()
                    }
                })
                .unwrap_or(Err(Error::Protocol("channel out of range")));
            let tag = tag?;
            let frames = match self.build_publish_frames(
                exchange,
                routing_key,
                mandatory,
                properties,
                body,
                state.negotiated().frame_max,
            ) {
                Ok(frames) => frames,
                Err(error) => {
                    state.with_channel(self.channel, |channel| channel.cancel_publish(tag));
                    return Err(error);
                }
            };
            if state.try_enqueue(frames).is_err() {
                state.with_channel(self.channel, |channel| channel.cancel_publish(tag));
                return Err(Error::Other("egress queue full".into()));
            }
            Ok(tag)
        });
        let tag = match setup {
            None => return PublishConfirmFuture::failed(self.handle, self.channel, Error::Closed),
            Some(Err(error)) => {
                return PublishConfirmFuture::failed(self.handle, self.channel, error);
            }
            Some(Ok(tag)) => tag,
        };
        PublishConfirmFuture::confirmed(self.handle, self.channel, tag)
    }
}
