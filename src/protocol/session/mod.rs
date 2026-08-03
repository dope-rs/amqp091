use super::*;
use dope::manifold::connector::session;

mod content;
pub mod factory;
mod method;
pub mod outbound;
pub mod port;

use self::content::SessionContent;
use self::method::SessionMethod;

pub struct Session<'d> {
    codec: Codec,
    config: &'d Config,
    port: &'d AmqpPort<'d>,
}

impl<'d> Session<'d> {
    pub fn new(port: &'d AmqpPort<'d>) -> Self {
        Self {
            codec: Codec::new(port.config().frame_max),
            config: port.config(),
            port,
        }
    }

    pub fn config(&self) -> &Config {
        self.config
    }

    pub(crate) fn port(&self) -> &'d AmqpPort<'d> {
        self.port
    }

    pub(in crate::protocol) fn with_shared<R>(
        &self,
        f: impl for<'a> FnOnce(&'a mut Shared<'d>) -> R,
    ) -> R {
        self.port.with_shared(f)
    }

    pub(in crate::protocol) fn send_method<S: ClientMethod>(
        out: &mut Queue<'_, 'd, '_, { IOV_CAP }, Outbound<'d>>,
        region: &mut RegionToken<'d>,
        channel: u16,
        m: &S,
    ) -> Result<(), Error> {
        let mut stage = out.wire_stage(region);
        m.write_frame(&mut stage, channel);
        if stage.overflowed() {
            return Err(Error::CapacityOverflow("wire buffer"));
        }
        stage.commit();
        Ok(())
    }
}

#[dope_gen::connector_session(codec = codec, io = port.io)]
impl<'d> session::Session<'d> for Session<'d> {
    type Codec = Codec;
    type ConnState = ConnState<'d>;
    type Send = Outbound<'d>;

    fn connect(&mut self, ctx: &mut Ctx<'_, '_, 'd, Self>) {
        let mut stage = ctx.sink.wire_stage(ctx.region);
        FrameEncoder::new(&mut stage).write_protocol_header();
        assert!(stage.commit() != 0);
        ctx.state.phase = ConnPhase::AwaitingStart;
        ctx.state.pending_close = false;
        ctx.state.close_permanent = false;
        let channels = self.with_shared(|shared| {
            shared.fatal = None;
            shared.ready_conn = None;
            &raw const shared.channels
        });
        ctx.state.channels.set(channels);
    }

    fn response(&mut self, head: Frame, ctx: &mut Ctx<'_, '_, 'd, Self>) {
        self.with_shared(|shared| shared.last_recv_at = Some(Instant::now()));
        if head.oversize {
            self.fail_session(
                ctx.state,
                Error::FrameTooLarge {
                    size: head.raw_payload_len.saturating_add(OVERHEAD as u32),
                    max: self.codec.max_frame(),
                },
                true,
            );
            return;
        }
        if head.end_byte != FrameType::END {
            self.fail_session(ctx.state, Error::Protocol("bad frame-end marker"), true);
            return;
        }
        let result = match head.frame_type {
            FrameType::METHOD => {
                self.handle_method(head, ctx.conn_id, ctx.state, &mut ctx.sink, ctx.region)
            }
            FrameType::HEADER => self.handle_header(head, ctx.state),
            FrameType::BODY => self.handle_body(head, ctx.state),
            FrameType::HEARTBEAT => Ok(()),
            other => Err(Error::ProtocolOwned(format!("unknown frame type {other}"))),
        };
        if let Err(e) = result {
            let permanent = matches!(
                e,
                Error::Protocol(_)
                    | Error::ProtocolOwned(_)
                    | Error::Auth(_)
                    | Error::UnsupportedMechanism
                    | Error::UnknownClass(_)
                    | Error::UnknownMethod { .. }
                    | Error::MessageTooLarge { .. }
                    | Error::CapacityOverflow(_)
            );
            self.fail_session(ctx.state, e, permanent);
        }
    }

    fn disconnect(&mut self, ctx: &mut Ctx<'_, '_, 'd, Self>) {
        self.port.io.deactivate(ctx.region, ctx.conn_id);
        let err = self.with_shared(|shared| shared.fatal.clone().unwrap_or(Error::Closed));
        ctx.state.phase = ConnPhase::Closed;
        ctx.state.pending_close = false;
        self.with_shared(|shared| shared.ready_conn = None);
        ctx.state.for_each_channel_mut(|c| {
            c.phase = ChannelPhase::Closed;
            c.clear_inbound();
            c.fail_all(&err);
        });
        self.with_shared(|shared| {
            shared.handshake_wakers.wake();
            shared.closed_wakers.wake();
        });
    }

    fn pre_park(&mut self, region: &mut RegionToken<'d>) {
        loop {
            let Some((target, frame)) =
                self.with_shared(|shared| shared.pending_egress.pop_front())
            else {
                break;
            };
            if let Err(frame) = self.port.io.try_enqueue(region, target, frame)
                && self.port.io.is_active(target)
            {
                let restored = self.with_shared(|shared| {
                    shared.pending_egress.push_front((target, frame)).is_ok()
                });
                debug_assert!(restored, "popped handoff slot must remain available");
                break;
            }
        }
    }
}
