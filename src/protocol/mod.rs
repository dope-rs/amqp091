use std::cell::{Cell, UnsafeCell};
use std::mem::{replace, take, transmute};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::Poll;
use std::time::Instant;

use dope::driver::token::Token;
use dope::manifold::connector::state::{IOV_CAP, Queue};
use dope::manifold::connector::{self, Close, Ctx, Port};
use dope::manifold::timer::Timer;
use dope::runtime::{Idle, StorageFactory};
use dope_fiber::Waker;
use o3::buffer::{Lease, Pool, PoolLayout};
use o3::collections::{FixedHashTable, FixedQueue, Slab, SlabKey, SlotQueue};
use o3::mem::{ByteBudget, ByteBudgetHandle};

use crate::config::{ChannelCapacity, Config};
use crate::content::properties::{ContentHeader, Delivery, Returned};
use crate::content::{Body, BodyPool};
use crate::error::Error;
use crate::method::connection::{Open, StartOk, TuneOk};
use crate::method::core::ClientMethod;
use crate::method::{Server, basic, channel, connection, queue};
use crate::runtime::wake::{WakeRegistry, WakeSlot};
use crate::topology::Topology;
use crate::wire::frame::{Codec, Frame, FrameEncoder, FrameType, OVERHEAD};
use crate::wire::sink::Sink;

const NONE: u32 = u32::MAX;

pub(crate) mod channels;
pub mod connections;
pub mod session;

use self::channels::confirm::ConfirmTracker;
use self::channels::consumer::{ConsumerSlot, ConsumerTable, DeliveryNode};
use self::channels::messages::ChannelMessages;
use self::channels::state::{ChannelPhase, ChannelState, InboundContent, PendingMethod};
use self::connections::channels::Channels;
use self::connections::free::FreeChannels;
use self::connections::limits::Limits;
use self::connections::shared::Shared;
use self::connections::state::{ConnPhase, ConnState};
use self::session::Session;
use self::session::factory::AmqpPortFactory;
use self::session::outbound::Outbound;
use self::session::port::AmqpPort;

pub(crate) struct PendingMethodOp<'d> {
    pub(crate) expect: ExpectMethod,
    pub(crate) frame: Option<Outbound>,
    pub(crate) waker: WakeSlot<'d>,
    pub(crate) detached: bool,
    pub(crate) sent: bool,
    pub(crate) outcome: Option<Result<PendingResponse, Error>>,
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
    BasicConsumeOk(basic::ConsumeOk, SlabKey),
    BasicCancelOk(basic::CancelOk),
}

#[derive(Default, Clone, Copy)]
pub struct Negotiated {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}
