use super::*;
use crate::method::channel::Close;

pub mod consume;
pub mod exchanges;
pub mod publish;
pub mod queues;

pub struct Channel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub(super) handle: AmqpHandle<'a, 'd, S, E>,
    pub(super) channel: u16,
}

impl<'a, 'd, S, E> Channel<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn id(&self) -> u16 {
        self.channel
    }

    pub fn handle(&self) -> AmqpHandle<'a, 'd, S, E> {
        self.handle
    }

    pub fn wait_flow_resumed(&self) -> WaitFlowResumed<'a, 'd, S, E> {
        WaitFlowResumed::new(self.handle, self.channel)
    }

    pub fn next_return(&self) -> NextReturn<'a, 'd, S, E> {
        NextReturn::new(self.handle, self.channel)
    }

    pub(super) fn send_for<M: ClientMethod, R>(
        &self,
        expect: ExpectMethod,
        method: M,
    ) -> MethodFuture<'a, 'd, S, E, R>
    where
        R: Extract,
    {
        let frame = self
            .handle
            .port
            .encode(|out| method.write_frame(out, self.channel));
        MethodFuture::new(self.handle, self.channel, expect, frame)
    }

    pub fn close(&self, reply_code: u16, reply_text: &str) -> MethodFuture<'a, 'd, S, E, ()> {
        self.send_for::<_, ()>(
            ExpectMethod::ChannelCloseOk,
            Close {
                reply_code,
                reply_text: reply_text.into(),
                class_id: 0,
                method_id: 0,
            },
        )
    }

    pub(super) fn build_publish_frames(
        &self,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: &BasicProperties,
        body: &[u8],
        frame_max: u32,
    ) -> Result<Outbound<'d>, Error> {
        let max_body = Self::body_payload_max(frame_max);
        self.handle.port.encode(|out| {
            basic::Publish {
                exchange,
                routing_key,
                mandatory,
                immediate: false,
            }
            .write_frame(out, self.channel);
            FrameEncoder::new(out).write(FrameType::HEADER, self.channel, |sink| {
                properties.encode_header_frame(sink, body.len() as u64);
            });
            let mut offset = 0;
            while offset < body.len() {
                let end = (offset + max_body).min(body.len());
                FrameEncoder::new(out).write_raw_body(self.channel, &body[offset..end]);
                offset = end;
            }
        })
    }

    fn body_payload_max(frame_max: u32) -> usize {
        if frame_max == 0 {
            4096
        } else {
            (frame_max as usize).saturating_sub(OVERHEAD).max(1)
        }
    }

    pub(super) fn send_fire_forget<M: ClientMethod>(&self, method: &M) -> bool {
        let conn = self.handle.connector();
        let target = conn.session().with_shared(|shared| {
            shared
                .fatal
                .is_none()
                .then_some(shared.ready_conn)
                .flatten()
        });
        let Some(target) = target else {
            return false;
        };
        let Ok(frame) = self
            .handle
            .port
            .encode(|out| method.write_frame(out, self.channel))
        else {
            return false;
        };
        conn.with_state(target, |state| state.try_enqueue(frame).is_ok())
            .unwrap_or(false)
    }

    pub(super) fn with_topology<R, F: FnOnce(&mut Topology, u16) -> R>(&self, f: F) -> R {
        self.handle
            .connector()
            .session()
            .with_shared(|shared| f(shared.topology_mut(), self.channel))
    }
}
