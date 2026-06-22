use std::collections::{BTreeSet, VecDeque};
use std::time::Instant;

use dope::manifold::connector::{self, Close, Ctx};
use dope::runtime::token::Token;
use dope::{WakeRef, WakerSet};
use o3::buffer;
use o3::buffer::Owned;

use crate::Error;
use crate::config::{Config, Mechanism};
use crate::content::{ContentHeader, Delivery, Returned};
use crate::method::connection::{AuthResponse, Open, StartOk, TuneOk};
use crate::method::{ClientMethod, Server, basic, channel, connection, queue};
use crate::wire::{Codec, Frame, FrameEncoder, FrameType, PROTOCOL_HEADER};

const MAX_CHANNELS: usize = 256;
const BODY_PREALLOC_CAP: usize = 1 << 20;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
enum ConnPhase {
    #[default]
    NeedsHello,
    AwaitingStart,
    AwaitingTune,
    AwaitingOpenOk,
    Ready,
    Closed,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelPhase {
    #[default]
    Closed,
    Open,
}

#[derive(Debug, Default)]
enum InboundContent {
    #[default]
    Idle,
    AwaitingHeader(PendingMethod),
    AwaitingBody {
        method: PendingMethod,
        header: Box<ContentHeader>,
        body: Vec<u8>,
    },
}

#[derive(Debug)]
enum PendingMethod {
    Deliver(basic::Deliver),
    Return(basic::Return),
}

pub(super) struct ChannelState {
    phase: ChannelPhase,
    inbound: InboundContent,
    pending_ops: VecDeque<PendingMethodOp>,
    completed: VecDeque<(u64, Result<PendingResponse, Error>)>,
    next_op_id: u64,
    consumers: Vec<ConsumerSlot>,
    confirm: Option<ConfirmTracker>,
    flow_active: bool,
    returns: VecDeque<Returned>,
    return_wakers: WakerSet,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            phase: ChannelPhase::default(),
            inbound: InboundContent::default(),
            pending_ops: VecDeque::new(),
            completed: VecDeque::new(),
            next_op_id: 0,
            consumers: Vec::new(),
            confirm: None,
            flow_active: true,
            returns: VecDeque::new(),
            return_wakers: WakerSet::new(),
        }
    }
}

pub(super) struct PendingMethodOp {
    pub(super) op_id: u64,
    pub(super) expect: ExpectMethod,
    pub(super) frame: Option<buffer::Shared>,
    pub(super) wakers: WakerSet,
    pub(super) result: Option<Result<PendingResponse, Error>>,
}

#[derive(Default)]
struct ConfirmTracker {
    next_tag: u64,
    pending: VecDeque<PendingPublish>,
}

struct PendingPublish {
    tag: u64,
    wakers: WakerSet,
    outcome: Option<PublishConfirm>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishConfirm {
    Ack,
    Nack { requeue: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectMethod {
    ChannelOpenOk,
    ChannelCloseOk,
    QueueDeclareOk,
    QueueBindOk,
    QueueUnbindOk,
    QueuePurgeOk,
    QueueDeleteOk,
    ExchangeDeclareOk,
    ExchangeDeleteOk,
    ExchangeBindOk,
    ExchangeUnbindOk,
    BasicQosOk,
    BasicConsumeOk,
    BasicCancelOk,
    BasicRecoverOk,
    ConfirmSelectOk,
    TxSelectOk,
    TxCommitOk,
    TxRollbackOk,
}

#[derive(Debug)]
pub enum PendingResponse {
    Empty,
    QueueDeclareOk(queue::DeclareOk),
    QueuePurgeOk(queue::PurgeOk),
    QueueDeleteOk(queue::DeleteOk),
    BasicConsumeOk(basic::ConsumeOk),
    BasicCancelOk(basic::CancelOk),
}

struct ConsumerSlot {
    tag: String,
    deliveries: VecDeque<Delivery>,
    wakers: WakerSet,
    canceled: bool,
}

impl ChannelState {
    pub(super) fn flow_active(&self) -> bool {
        self.flow_active
    }

    pub(super) fn set_flow_active(&mut self, active: bool) {
        self.flow_active = active;
    }

    pub(super) fn enqueue_op(
        &mut self,
        expect: ExpectMethod,
        frame: buffer::Shared,
    ) -> (u64, bool) {
        let op_id = self.next_op_id;
        self.next_op_id = self.next_op_id.wrapping_add(1);
        let was_empty = self.pending_ops.is_empty();
        let stored_frame = if was_empty { None } else { Some(frame) };
        self.pending_ops.push_back(PendingMethodOp {
            op_id,
            expect,
            frame: stored_frame,
            wakers: WakerSet::new(),
            result: None,
        });
        (op_id, was_empty)
    }

    pub(super) fn poll_op(
        &mut self,
        op_id: u64,
        w: WakeRef,
    ) -> Option<Result<PendingResponse, Error>> {
        if let Some(pos) = self.completed.iter().position(|(id, _)| *id == op_id) {
            return Some(self.completed.remove(pos)?.1);
        }
        let pos = self.pending_ops.iter().position(|o| o.op_id == op_id)?;
        if self.pending_ops[pos].result.is_some() {
            let removed = self.pending_ops.remove(pos)?;
            return removed.result;
        }
        self.pending_ops[pos].wakers.register(w);
        None
    }

    pub(super) fn cancel_op(&mut self, op_id: u64) {
        if let Some(pos) = self.pending_ops.iter().position(|o| o.op_id == op_id) {
            self.pending_ops.remove(pos);
        }
        if let Some(pos) = self.completed.iter().position(|(id, _)| *id == op_id) {
            self.completed.remove(pos);
        }
    }

    pub(super) fn register_consumer(&mut self, tag: String) {
        if let Some(existing) = self.consumers.iter_mut().find(|c| c.tag == tag) {
            existing.canceled = false;
            existing.deliveries.clear();
            return;
        }
        self.consumers.push(ConsumerSlot {
            tag,
            deliveries: VecDeque::new(),
            wakers: WakerSet::new(),
            canceled: false,
        });
    }

    pub(super) fn reset_for_reopen(&mut self) {
        self.inbound = InboundContent::Idle;
        self.consumers.clear();
        self.confirm = None;
        self.flow_active = true;
        self.returns.clear();
    }

    pub(super) fn consumer_next(&mut self, tag: &str, w: WakeRef) -> ConsumerPoll {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.tag == tag) {
            if let Some(d) = c.deliveries.pop_front() {
                return ConsumerPoll::Ready(Box::new(d));
            }
            if c.canceled {
                return ConsumerPoll::Canceled;
            }
            c.wakers.register(w);
            ConsumerPoll::Pending
        } else {
            ConsumerPoll::Canceled
        }
    }

    pub(super) fn complete_head_with(
        &mut self,
        expect: ExpectMethod,
        response: PendingResponse,
    ) -> Result<Option<buffer::Shared>, Error> {
        let Some(head) = self.pending_ops.front_mut() else {
            return Ok(None);
        };
        if head.expect != expect {
            return Err(Error::Protocol("response method does not match pending op"));
        }
        let op_id = head.op_id;
        head.wakers.drain_wake();
        self.pending_ops.pop_front();
        self.completed.push_back((op_id, Ok(response)));
        if let Some(next) = self.pending_ops.front_mut() {
            return Ok(next.frame.take());
        }
        Ok(None)
    }

    fn route_delivery(&mut self, d: Delivery) {
        if let Some(c) = self
            .consumers
            .iter_mut()
            .find(|c| c.tag == d.method.consumer_tag)
        {
            c.deliveries.push_back(d);
            c.wakers.drain_wake();
        }
    }

    fn route_return(&mut self, r: Returned) {
        self.returns.push_back(r);
        self.return_wakers.drain_wake();
    }

    pub(super) fn next_return(&mut self, w: WakeRef) -> Option<Returned> {
        if let Some(r) = self.returns.pop_front() {
            return Some(r);
        }
        self.return_wakers.register(w);
        None
    }

    fn cancel_consumer(&mut self, tag: &str) {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.tag == tag) {
            c.canceled = true;
            c.wakers.drain_wake();
        }
    }

    fn fail_all(&mut self, err: &Error) {
        for op in self.pending_ops.iter_mut() {
            if op.result.is_none() {
                op.result = Some(Err(err.clone()));
            }
            op.wakers.drain_wake();
        }
        for c in &mut self.consumers {
            c.canceled = true;
            c.wakers.drain_wake();
        }
        if let Some(tracker) = &mut self.confirm {
            for mut p in tracker.pending.drain(..) {
                p.wakers.drain_wake();
            }
        }
        self.return_wakers.drain_wake();
    }

    pub(super) fn enable_confirm(&mut self) {
        if self.confirm.is_none() {
            self.confirm = Some(ConfirmTracker::default());
        }
    }

    pub(super) fn confirm_enabled(&self) -> bool {
        self.confirm.is_some()
    }

    pub(super) fn next_publish_tag(&mut self) -> Option<u64> {
        let tracker = self.confirm.as_mut()?;
        tracker.next_tag += 1;
        tracker.pending.push_back(PendingPublish {
            tag: tracker.next_tag,
            wakers: WakerSet::new(),
            outcome: None,
        });
        Some(tracker.next_tag)
    }

    pub(super) fn poll_publish(
        &mut self,
        tag: u64,
        w: WakeRef,
    ) -> Result<Option<PublishConfirm>, Error> {
        let tracker = self
            .confirm
            .as_mut()
            .ok_or(Error::Protocol("publisher confirms not enabled"))?;
        let Some(idx) = tracker.pending.iter().position(|p| p.tag == tag) else {
            return Err(Error::Protocol("unknown publish delivery tag"));
        };
        if let Some(outcome) = tracker.pending[idx].outcome {
            tracker.pending.remove(idx);
            return Ok(Some(outcome));
        }
        tracker.pending[idx].wakers.register(w);
        Ok(None)
    }

    fn confirm_publishes(&mut self, tag: u64, multiple: bool, outcome: PublishConfirm) {
        let Some(tracker) = &mut self.confirm else {
            return;
        };
        if multiple {
            for p in tracker.pending.iter_mut().filter(|p| p.tag <= tag) {
                if p.outcome.is_none() {
                    p.outcome = Some(outcome);
                    p.wakers.drain_wake();
                }
            }
        } else if let Some(p) = tracker.pending.iter_mut().find(|p| p.tag == tag)
            && p.outcome.is_none()
        {
            p.outcome = Some(outcome);
            p.wakers.drain_wake();
        }
    }
}

pub(super) enum ConsumerPoll {
    Ready(Box<Delivery>),
    Pending,
    Canceled,
}

#[derive(Default)]
pub struct Negotiated {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Default)]
pub struct Shared {
    pub(super) ready_conn: Option<Token>,
    pub(super) handshake_wakers: WakerSet,
    pub(super) fatal: Option<Error>,
    pub(super) closed_wakers: WakerSet,
    pub(super) progress_wakers: WakerSet,
    pub(super) last_recv_at: Option<Instant>,
    pub(super) topology: crate::topology::Topology,
    pub(super) auto_recovery_enabled: bool,
    pub(super) intentionally_closed: bool,
    pub(super) ready_generation: u64,
    pub(super) blocked: bool,
    pub(super) blocked_reason: String,
    pub(super) allocated_channels: Option<BTreeSet<u16>>,
}

impl Shared {
    pub fn is_ready(&self) -> bool {
        self.ready_conn.is_some() && self.fatal.is_none()
    }

    pub fn is_failed(&self) -> bool {
        self.fatal.is_some()
    }

    pub fn ready_conn(&self) -> Option<Token> {
        self.ready_conn
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked
    }

    pub fn blocked_reason(&self) -> &str {
        &self.blocked_reason
    }

    pub fn register_handshake_waker(&mut self, w: WakeRef) {
        self.handshake_wakers.register(w);
    }

    pub fn register_closed_waker(&mut self, w: WakeRef) {
        self.closed_wakers.register(w);
    }

    pub fn register_progress_waker(&mut self, w: WakeRef) {
        self.progress_wakers.register(w);
    }

    pub fn ready_generation(&self) -> u64 {
        self.ready_generation
    }

    pub fn auto_recovery_enabled(&self) -> bool {
        self.auto_recovery_enabled
    }

    pub fn intentionally_closed(&self) -> bool {
        self.intentionally_closed
    }

    pub(super) fn enable_auto_recovery(&mut self) {
        self.auto_recovery_enabled = true;
    }

    pub(super) fn clear_fatal(&mut self) {
        self.fatal = None;
        self.handshake_wakers.drain_wake();
    }

    pub(super) fn mark_intentional_close(&mut self) {
        self.intentionally_closed = true;
    }

    pub(super) fn topology_mut(&mut self) -> &mut crate::topology::Topology {
        &mut self.topology
    }

    pub fn topology(&self) -> &crate::topology::Topology {
        &self.topology
    }

    pub fn next_unused_channel(&mut self, channel_max: u16) -> Option<u16> {
        let upper = if channel_max == 0 {
            (MAX_CHANNELS - 1).min(u16::MAX as usize) as u16
        } else {
            channel_max
        };
        let set = self.allocated_channels.get_or_insert_with(BTreeSet::new);
        let mut id: u16 = 1;
        while id <= upper {
            if !set.contains(&id) {
                set.insert(id);
                return Some(id);
            }
            if id == u16::MAX {
                break;
            }
            id += 1;
        }
        None
    }

    pub fn release_channel(&mut self, id: u16) {
        if let Some(set) = self.allocated_channels.as_mut() {
            set.remove(&id);
        }
    }

    pub(super) fn reserve_channel(&mut self, id: u16) -> bool {
        let set = self.allocated_channels.get_or_insert_with(BTreeSet::new);
        set.insert(id)
    }

    pub(super) fn record_failure(&mut self, err: Error) {
        if self.fatal.is_none() {
            self.fatal = Some(err);
        }
        self.ready_conn = None;
        self.handshake_wakers.drain_wake();
        self.closed_wakers.drain_wake();
    }

    pub(super) fn record_recoverable_disconnect(&mut self) {
        self.ready_conn = None;
        self.handshake_wakers.drain_wake();
        self.closed_wakers.drain_wake();
        self.progress_wakers.drain_wake();
    }

    pub(super) fn clear_dead_ready_conn(&mut self) -> bool {
        if self.ready_conn.take().is_some() {
            self.handshake_wakers.drain_wake();
            self.closed_wakers.drain_wake();
            self.progress_wakers.drain_wake();
            true
        } else {
            false
        }
    }
}

#[derive(Default)]
pub struct ConnState {
    phase: ConnPhase,
    pub negotiated: Negotiated,
    channels: Vec<ChannelState>,
    pending_close: bool,
    close_permanent: bool,
}

impl ConnState {
    fn ensure_channels(&mut self, n: usize) {
        if self.channels.len() < n {
            self.channels.resize_with(n, ChannelState::default);
        }
    }

    pub(super) fn channel_mut(&mut self, id: u16) -> Option<&mut ChannelState> {
        self.channels.get_mut(id as usize)
    }

    pub fn is_ready(&self) -> bool {
        self.phase == ConnPhase::Ready
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.phase, ConnPhase::Closed)
    }
}

impl connector::Lifecycle for ConnState {
    fn wants_close(&self) -> Close {
        if !self.pending_close {
            Close::Keep
        } else if self.close_permanent {
            Close::Permanent
        } else {
            Close::Reconnect
        }
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}

pub struct Session {
    codec: Codec,
    config: Config,
    pub shared: Shared,
}

impl Session {
    pub fn new(config: Config) -> Self {
        Self {
            codec: Codec,
            config,
            shared: Shared::default(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    fn send_method<S: ClientMethod>(
        out: &mut connector::session::Queue<{ connector::session::IOV_CAP }>,
        channel: u16,
        m: &S,
    ) {
        let mut buf = Owned::with_capacity(64);
        m.write_frame(&mut buf, channel);
        out.push(buf.freeze());
    }

    fn handle_method(
        &mut self,
        head: Frame,
        conn_id: Token,
        state: &mut ConnState,
        out: &mut connector::session::Queue<{ connector::session::IOV_CAP }>,
    ) -> Result<(), Error> {
        let server = Server::parse(&head.payload)?;
        let channel = head.channel;

        if matches!(state.phase, ConnPhase::AwaitingStart)
            && let Server::ConnectionStart(start) = &server
        {
            return self.on_connection_start(start, state, out);
        }
        if matches!(state.phase, ConnPhase::AwaitingTune)
            && let Server::ConnectionTune(tune) = &server
        {
            return self.on_connection_tune(tune, state, out);
        }
        if matches!(state.phase, ConnPhase::AwaitingOpenOk)
            && let Server::ConnectionOpenOk(_) = &server
        {
            state.phase = ConnPhase::Ready;
            self.shared.ready_conn = Some(conn_id);
            self.shared.fatal = None;
            self.shared.ready_generation = self.shared.ready_generation.saturating_add(1);
            self.shared.handshake_wakers.drain_wake();
            return Ok(());
        }

        match server {
            Server::ConnectionStart(_)
            | Server::ConnectionTune(_)
            | Server::ConnectionOpenOk(_) => Err(Error::Protocol(
                "unexpected connection setup method in current phase",
            )),
            Server::ConnectionSecure(_) => Err(Error::Auth(
                "SASL challenge mid-conversation not supported".into(),
            )),
            Server::ConnectionClose(close) => {
                Self::send_method(out, 0, &connection::CloseOk);
                state.phase = ConnPhase::Closed;
                state.pending_close = true;
                let recoverable = Error::connection_close_is_recoverable(close.reply_code);
                state.close_permanent = !recoverable;
                let err = Error::Server {
                    reply_code: close.reply_code,
                    reply_text: close.reply_text,
                    class_id: close.class_id,
                    method_id: close.method_id,
                };
                for c in &mut state.channels {
                    c.phase = ChannelPhase::Closed;
                    c.inbound = InboundContent::Idle;
                    c.fail_all(&err);
                }
                if recoverable {
                    self.shared.record_recoverable_disconnect();
                } else {
                    self.shared.record_failure(err);
                }
                Ok(())
            }
            Server::ConnectionCloseOk(_) => {
                state.phase = ConnPhase::Closed;
                state.pending_close = true;
                state.close_permanent = false;
                self.shared.ready_conn = None;
                self.shared.closed_wakers.drain_wake();
                Ok(())
            }
            Server::ConnectionBlocked(b) => {
                self.shared.blocked = true;
                self.shared.blocked_reason = b.reason;
                Ok(())
            }
            Server::ConnectionUnblocked(_) => {
                self.shared.blocked = false;
                self.shared.blocked_reason.clear();
                self.shared.progress_wakers.drain_wake();
                Ok(())
            }
            Server::ChannelOpenOk(_) => {
                state.ensure_channels((channel as usize) + 1);
                if let Some(c) = state.channel_mut(channel) {
                    c.reset_for_reopen();
                    c.phase = ChannelPhase::Open;
                }
                Self::complete(
                    state,
                    channel,
                    ExpectMethod::ChannelOpenOk,
                    PendingResponse::Empty,
                    out,
                )?;
                Ok(())
            }
            Server::ChannelFlow(f) => {
                Self::send_method(out, channel, &channel::FlowOk { active: f.active });
                if let Some(c) = state.channel_mut(channel) {
                    c.set_flow_active(f.active);
                }
                if f.active {
                    self.shared.progress_wakers.drain_wake();
                }
                Ok(())
            }
            Server::ChannelFlowOk(_) => Ok(()),
            Server::ChannelClose(close) => {
                Self::send_method(out, channel, &channel::CloseOk);
                let err = Error::Server {
                    reply_code: close.reply_code,
                    reply_text: close.reply_text.clone(),
                    class_id: close.class_id,
                    method_id: close.method_id,
                };
                if let Some(c) = state.channel_mut(channel) {
                    c.phase = ChannelPhase::Closed;
                    c.inbound = InboundContent::Idle;
                    c.fail_all(&err);
                }
                Ok(())
            }
            Server::ChannelCloseOk(_) => {
                if let Some(c) = state.channel_mut(channel) {
                    c.phase = ChannelPhase::Closed;
                    c.inbound = InboundContent::Idle;
                }
                Self::complete(
                    state,
                    channel,
                    ExpectMethod::ChannelCloseOk,
                    PendingResponse::Empty,
                    out,
                )?;
                if let Some(allocated) = self.shared.allocated_channels.as_mut() {
                    allocated.remove(&channel);
                }
                Ok(())
            }
            Server::ExchangeDeclareOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::ExchangeDeclareOk,
                PendingResponse::Empty,
                out,
            ),
            Server::ExchangeDeleteOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::ExchangeDeleteOk,
                PendingResponse::Empty,
                out,
            ),
            Server::ExchangeBindOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::ExchangeBindOk,
                PendingResponse::Empty,
                out,
            ),
            Server::ExchangeUnbindOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::ExchangeUnbindOk,
                PendingResponse::Empty,
                out,
            ),
            Server::QueueDeclareOk(ok) => Self::complete(
                state,
                channel,
                ExpectMethod::QueueDeclareOk,
                PendingResponse::QueueDeclareOk(ok),
                out,
            ),
            Server::QueueBindOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::QueueBindOk,
                PendingResponse::Empty,
                out,
            ),
            Server::QueueUnbindOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::QueueUnbindOk,
                PendingResponse::Empty,
                out,
            ),
            Server::QueuePurgeOk(ok) => Self::complete(
                state,
                channel,
                ExpectMethod::QueuePurgeOk,
                PendingResponse::QueuePurgeOk(ok),
                out,
            ),
            Server::QueueDeleteOk(ok) => Self::complete(
                state,
                channel,
                ExpectMethod::QueueDeleteOk,
                PendingResponse::QueueDeleteOk(ok),
                out,
            ),
            Server::BasicQosOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::BasicQosOk,
                PendingResponse::Empty,
                out,
            ),
            Server::BasicConsumeOk(ok) => {
                let tag = ok.consumer_tag.clone();
                let res = Self::complete(
                    state,
                    channel,
                    ExpectMethod::BasicConsumeOk,
                    PendingResponse::BasicConsumeOk(ok),
                    out,
                );
                if let Some(c) = state.channel_mut(channel) {
                    c.register_consumer(tag);
                }
                self.shared.progress_wakers.drain_wake();
                res
            }
            Server::BasicCancelOk(ok) => {
                let tag = ok.consumer_tag.clone();
                let res = Self::complete(
                    state,
                    channel,
                    ExpectMethod::BasicCancelOk,
                    PendingResponse::BasicCancelOk(ok),
                    out,
                );
                if let Some(c) = state.channel_mut(channel) {
                    c.cancel_consumer(&tag);
                }
                res
            }
            Server::BasicCancel(c_msg) => {
                if let Some(c) = state.channel_mut(channel) {
                    c.cancel_consumer(&c_msg.consumer_tag);
                }
                Ok(())
            }
            Server::BasicRecoverOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::BasicRecoverOk,
                PendingResponse::Empty,
                out,
            ),
            Server::BasicAck(a) => {
                if let Some(c) = state.channel_mut(channel) {
                    c.confirm_publishes(a.delivery_tag, a.multiple, PublishConfirm::Ack);
                }
                Ok(())
            }
            Server::BasicNack(n) => {
                if let Some(c) = state.channel_mut(channel) {
                    c.confirm_publishes(
                        n.delivery_tag,
                        n.multiple,
                        PublishConfirm::Nack { requeue: n.requeue },
                    );
                }
                Ok(())
            }
            Server::BasicDeliver(d) => {
                state.ensure_channels((channel as usize) + 1);
                if let Some(c) = state.channel_mut(channel) {
                    c.inbound = InboundContent::AwaitingHeader(PendingMethod::Deliver(d));
                }
                Ok(())
            }
            Server::BasicReturn(rt) => {
                state.ensure_channels((channel as usize) + 1);
                if let Some(c) = state.channel_mut(channel) {
                    c.inbound = InboundContent::AwaitingHeader(PendingMethod::Return(rt));
                }
                Ok(())
            }
            Server::BasicGetOk(_) => Err(Error::Protocol(
                "unsolicited Basic.GetOk (Basic.Get not used)",
            )),
            Server::BasicGetEmpty(_) => Ok(()),
            Server::ConfirmSelectOk(_) => {
                if let Some(c) = state.channel_mut(channel) {
                    c.enable_confirm();
                }
                Self::complete(
                    state,
                    channel,
                    ExpectMethod::ConfirmSelectOk,
                    PendingResponse::Empty,
                    out,
                )
            }
            Server::TxSelectOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::TxSelectOk,
                PendingResponse::Empty,
                out,
            ),
            Server::TxCommitOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::TxCommitOk,
                PendingResponse::Empty,
                out,
            ),
            Server::TxRollbackOk(_) => Self::complete(
                state,
                channel,
                ExpectMethod::TxRollbackOk,
                PendingResponse::Empty,
                out,
            ),
        }
    }

    fn complete(
        state: &mut ConnState,
        channel: u16,
        expect: ExpectMethod,
        response: PendingResponse,
        out: &mut connector::session::Queue<{ connector::session::IOV_CAP }>,
    ) -> Result<(), Error> {
        let Some(c) = state.channel_mut(channel) else {
            return Ok(());
        };
        if let Some(next_frame) = c.complete_head_with(expect, response)? {
            out.push(next_frame);
        }
        Ok(())
    }

    fn on_connection_start(
        &mut self,
        start: &connection::Start,
        state: &mut ConnState,
        out: &mut connector::session::Queue<{ connector::session::IOV_CAP }>,
    ) -> Result<(), Error> {
        let mech_name = self.config.mechanism.name();
        if !start
            .mechanisms
            .split(' ')
            .any(|m| !m.is_empty() && m == mech_name)
        {
            return Err(Error::UnsupportedMechanism);
        }
        let response = match &self.config.mechanism {
            Mechanism::Plain => AuthResponse::plain(&self.config.user, &self.config.password),
            Mechanism::External { identity } => AuthResponse::external(identity),
        };
        let locale = if start
            .locales
            .split(' ')
            .any(|l| !l.is_empty() && l == self.config.locale)
        {
            self.config.locale.as_str()
        } else {
            start.locales.split(' ').next().unwrap_or("en_US")
        };
        Self::send_method(
            out,
            0,
            &StartOk {
                client_properties: &self.config.client_properties,
                mechanism: mech_name,
                response: &response,
                locale,
            },
        );
        state.phase = ConnPhase::AwaitingTune;
        Ok(())
    }

    fn on_connection_tune(
        &mut self,
        tune: &connection::Tune,
        state: &mut ConnState,
        out: &mut connector::session::Queue<{ connector::session::IOV_CAP }>,
    ) -> Result<(), Error> {
        let channel_max = Self::pick(self.config.channel_max, tune.channel_max);
        let frame_max = Self::pick_u32(self.config.frame_max, tune.frame_max);
        let heartbeat = Self::pick(self.config.heartbeat, tune.heartbeat);
        state.negotiated = Negotiated {
            channel_max,
            frame_max,
            heartbeat,
        };
        let n = (channel_max as usize + 1).min(MAX_CHANNELS);
        state.ensure_channels(n);
        Self::send_method(
            out,
            0,
            &TuneOk {
                channel_max,
                frame_max,
                heartbeat,
            },
        );
        Self::send_method(
            out,
            0,
            &Open {
                virtual_host: &self.config.virtual_host,
            },
        );
        state.phase = ConnPhase::AwaitingOpenOk;
        Ok(())
    }

    fn pick(client: u16, server: u16) -> u16 {
        match (client, server) {
            (0, s) => s,
            (c, 0) => c,
            (c, s) => c.min(s),
        }
    }

    fn pick_u32(client: u32, server: u32) -> u32 {
        match (client, server) {
            (0, s) => s,
            (c, 0) => c,
            (c, s) => c.min(s),
        }
    }

    fn handle_header(&mut self, head: Frame, state: &mut ConnState) -> Result<(), Error> {
        let header = ContentHeader::read(&head.payload)?;
        let channel = head.channel;
        let Some(c) = state.channel_mut(channel) else {
            return Err(Error::Protocol("HEADER frame on unknown channel"));
        };
        let prev = std::mem::replace(&mut c.inbound, InboundContent::Idle);
        let InboundContent::AwaitingHeader(method) = prev else {
            return Err(Error::Protocol("HEADER frame without preceding METHOD"));
        };
        if header.body_size == 0 {
            Self::finalize_message(state, channel, method, header, Vec::new())
        } else {
            let reserve = header.body_size.min(BODY_PREALLOC_CAP as u64) as usize;
            c.inbound = InboundContent::AwaitingBody {
                method,
                header: Box::new(header),
                body: Vec::with_capacity(reserve),
            };
            Ok(())
        }
    }

    fn handle_body(&mut self, head: Frame, state: &mut ConnState) -> Result<(), Error> {
        let channel = head.channel;
        let Some(c) = state.channel_mut(channel) else {
            return Err(Error::Protocol("BODY frame on unknown channel"));
        };
        let prev = std::mem::replace(&mut c.inbound, InboundContent::Idle);
        let InboundContent::AwaitingBody {
            method,
            header,
            mut body,
        } = prev
        else {
            return Err(Error::Protocol("BODY frame without preceding HEADER"));
        };
        body.extend_from_slice(&head.payload);
        let have = body.len() as u64;
        if have > header.body_size {
            return Err(Error::Protocol("content body exceeds declared body-size"));
        }
        if have == header.body_size {
            Self::finalize_message(state, channel, method, *header, body)
        } else {
            c.inbound = InboundContent::AwaitingBody {
                method,
                header,
                body,
            };
            Ok(())
        }
    }

    fn finalize_message(
        state: &mut ConnState,
        channel: u16,
        method: PendingMethod,
        header: ContentHeader,
        body: Vec<u8>,
    ) -> Result<(), Error> {
        if state.channel_mut(channel).is_none() {
            return Ok(());
        }
        match method {
            PendingMethod::Deliver(d) => {
                let delivery = Delivery {
                    method: d,
                    properties: header.properties,
                    body,
                };
                if let Some(c) = state.channel_mut(channel) {
                    c.route_delivery(delivery);
                }
            }
            PendingMethod::Return(rt) => {
                let msg = Returned {
                    method: rt,
                    properties: header.properties,
                    body,
                };
                if let Some(c) = state.channel_mut(channel) {
                    c.route_return(msg);
                }
            }
        }
        Ok(())
    }

    fn fail_session(&mut self, state: &mut ConnState, err: Error, permanent: bool) {
        state.phase = ConnPhase::Closed;
        state.pending_close = true;
        state.close_permanent = permanent;
        for c in &mut state.channels {
            c.phase = ChannelPhase::Closed;
            c.inbound = InboundContent::Idle;
            c.fail_all(&err);
        }
        self.shared.record_failure(err);
    }
}

impl connector::Session for Session {
    type Codec = Codec;
    type ConnState = ConnState;

    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn connect(&mut self, ctx: &mut Ctx<'_, Self>) {
        let mut buf = Owned::with_capacity(PROTOCOL_HEADER.len());
        FrameEncoder::write_protocol_header(&mut buf);
        ctx.sink.push(buf.freeze());
        ctx.state.phase = ConnPhase::AwaitingStart;
        ctx.state.pending_close = false;
        ctx.state.close_permanent = false;
        self.shared.fatal = None;
        self.shared.ready_conn = None;
    }

    fn response(&mut self, head: Frame, ctx: &mut Ctx<'_, Self>) {
        self.shared.last_recv_at = Some(Instant::now());
        if head.oversize {
            self.fail_session(
                ctx.state,
                Error::FrameTooLarge {
                    size: head.raw_payload_len,
                    max: crate::wire::MAX_FRAME_HARD_CAP,
                },
                true,
            );
            return;
        }
        let negotiated_max = ctx.state.negotiated.frame_max;
        if negotiated_max > 0 {
            let total = (head.payload.len() as u64) + (crate::wire::OVERHEAD as u64);
            if total > negotiated_max as u64 {
                self.fail_session(
                    ctx.state,
                    Error::FrameTooLarge {
                        size: head.payload.len() as u32,
                        max: negotiated_max,
                    },
                    true,
                );
                return;
            }
        }
        if head.end_byte != FrameType::END {
            self.fail_session(ctx.state, Error::Protocol("bad frame-end marker"), true);
            return;
        }
        let result = match head.frame_type {
            FrameType::METHOD => self.handle_method(head, ctx.conn_id, ctx.state, ctx.sink),
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
            );
            self.fail_session(ctx.state, e, permanent);
        }
    }

    fn disconnect(&mut self, ctx: &mut Ctx<'_, Self>) {
        let err = self.shared.fatal.clone().unwrap_or(Error::Closed);
        ctx.state.phase = ConnPhase::Closed;
        ctx.state.pending_close = false;
        self.shared.ready_conn = None;
        for c in &mut ctx.state.channels {
            c.phase = ChannelPhase::Closed;
            c.inbound = InboundContent::Idle;
            c.fail_all(&err);
        }
        self.shared.handshake_wakers.drain_wake();
        self.shared.closed_wakers.drain_wake();
    }
}
