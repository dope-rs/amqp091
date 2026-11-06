use std::{mem, num};

use o3::buffer::pool;
use o3::buffer::write::consume;
use o3::depot::index::bitmap;
use o3::mem::rc;

use crate::protocol::method;

mod capacity;
mod config;
mod encoding;
mod reply;

pub mod field;
pub mod property;
pub mod turn;

pub use capacity::Capacity;
pub use config::Config;
pub use encoding::Encoding;
pub(crate) use reply::Reply;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encode {
    Length {
        kind: Encoding,
        actual: usize,
        max: usize,
    },
    Sum {
        kind: Encoding,
        left: u32,
        right: u32,
        max: u32,
    },
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Allocation {
    Short,
    Queue,
    Exchange,
    VirtualHost,
    BrokerQueue,
    BrokerConsumer,
    FieldTable,
    Property,
    Content,
    Topology,
    Outbound,
    Authentication,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resident {
    Capacity {
        attempted: num::NonZero<usize>,
        capacity: usize,
    },
    Budget {
        required: num::NonZero<usize>,
        available: usize,
    },
    Layout(pool::LayoutError),
    Retain(rc::Error),
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consumer {
    New,
    Replay,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnMethod {
    Start,
    Secure,
    Tune,
    Open,
    Close,
    CloseOk,
    Blocked,
    Unblocked,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnPhase {
    Hello,
    Start,
    Tune,
    Open,
    Ready,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChanPhase {
    Closed,
    Opening,
    Open,
    Closing,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Replay {
    Confirm,
    Qos,
    Exchange,
    Queue,
    QueueBind,
    ExchangeBind,
    Current,
    Consumer,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Challenge {
        actual: num::NonZero<usize>,
    },
    TextNul {
        kind: Encoding,
        offset: usize,
    },
    Reserved {
        method: method::Id,
        actual: usize,
    },
    GetUnsolicited {
        actual: method::Kind,
    },
    ContentInterrupted,
    ContentClass {
        actual: u16,
    },
    ContentWeight {
        actual: u16,
    },
    ContentSize(consume::Error),
    Property(property::Fault),
    EncodedRange {
        start: u32,
        end: u32,
        available: usize,
    },
    EncodedField {
        field: u8,
        offset: u32,
        length: u8,
        available: usize,
    },
    EncodedOrder {
        field: u8,
        previous: usize,
        actual: u32,
    },
    EncodedSuffix {
        offset: u32,
        available: usize,
    },
    ContentHeaderDuplicate {
        properties: bool,
        body: bool,
    },
    ContentBodyUnbound {
        actual: usize,
    },
    ContentBodyOverflow(consume::Error),
    ContentMethodClosed,
    ContentMethodOpening,
    ContentMethodClosing,
    ContentMethodPending {
        properties: bool,
        body: bool,
    },
    ContentHeaderIdle,
    ContentBodyIdle,
    TuneFrame {
        actual: u32,
        min: u32,
    },
    MethodSize(consume::Error),
    ConnMethod {
        method: ConnMethod,
        phase: ConnPhase,
        closing: bool,
    },
    ChanMethod {
        channel: u16,
        method: method::Kind,
        phase: ChanPhase,
    },
    Consumer {
        expected: Consumer,
        actual: Consumer,
    },
    ConsumerOwner,
    ConsumerExpired {
        expected: num::NonZero<u64>,
        actual: Option<num::NonZero<u64>>,
    },
    ConsumerLease {
        expected: num::NonZero<u64>,
        actual: num::NonZero<u64>,
    },
    Response {
        expected: method::Kind,
        actual: method::Kind,
    },
    ConsumerKey {
        expected: Option<num::NonZero<u64>>,
        actual: Option<num::NonZero<u64>>,
        step: turn::Step,
    },
    ConsumerIndex {
        consumer: num::NonZero<u64>,
        expected: turn::Index,
        actual: turn::Index,
    },
    ConsumerPhase {
        consumer: num::NonZero<u64>,
        expected: turn::Consumer,
        actual: turn::Consumer,
    },
    MethodState {
        method: Option<num::NonZero<u64>>,
        expected: turn::Method,
        actual: turn::Method,
    },
    MethodOrder {
        expected: num::NonZero<u64>,
        actual: Option<num::NonZero<u64>>,
    },
    MethodControl {
        method: num::NonZero<u64>,
        expected: turn::Control,
        actual: turn::Control,
    },
    FlightMethod {
        expected: num::NonZero<u64>,
        actual: num::NonZero<u64>,
    },
    FlightState {
        method: num::NonZero<u64>,
        expected: turn::Use,
        actual: turn::Flight,
    },
    WorkReply {
        expected_method: method::Kind,
        actual_method: method::Kind,
        expected_reply: turn::Reply,
        actual_reply: turn::Reply,
    },
    QueueIdentity {
        actual: usize,
    },
    QueueNameEmpty,
    TopologyIdentity {
        queue: num::NonZero<u64>,
        expected: turn::Identity,
        actual: turn::Identity,
    },
    TopologyQueue {
        queue: num::NonZero<u64>,
    },
    ChanResponse {
        channel: u16,
        expected: Option<method::Kind>,
        actual: method::Kind,
    },
    ReplayResponse {
        expected: Option<num::NonZero<u64>>,
        actual: num::NonZero<u64>,
    },
    ReplayPhase {
        method: method::Kind,
        phase: ChanPhase,
    },
    ReplayRoute {
        stage: Replay,
        channel: u16,
        identity: num::NonZero<u64>,
    },
    ConfirmPublishDisabled,
    ConfirmPublishRequired,
    ConfirmAckDisabled {
        tag: u64,
        multiple: bool,
    },
    ConfirmTag {
        tag: u64,
        published: u64,
        multiple: bool,
    },
    CloseClosed,
    ChannelNotReady {
        channel: u16,
        phase: ConnPhase,
        closing: bool,
    },
    ChannelUnknown {
        channel: u16,
    },
    ControlRoute {
        channel: u16,
        expected: num::NonZero<u64>,
        actual: Option<num::NonZero<u64>>,
    },
    ControlMethod {
        channel: u16,
        expected: num::NonZero<u64>,
        actual: Option<num::NonZero<u64>>,
    },
    ConnectionChannel {
        channel: u16,
    },
    ChannelZero,
    ChannelBounds(bitmap::Error),
    ChannelMax {
        channel: u16,
        max: u16,
    },
    DeliveryTagZero,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Delivery,
    Returned,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    Control,
    Compact,
    Outbound,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    Method,
    Confirm,
    Consumer,
    Delivery,
    Returned,
    MethodStore,
    ControlLane,
    ControlFrame,
    Exchange,
    Queue,
    QueueBind,
    ExchangeBind,
}

const _: () = assert!(std::mem::size_of::<Encoding>() == 1);
const _: () = assert!(std::mem::size_of::<Allocation>() == 1);
const _: () = assert!(std::mem::size_of::<Resident>() == 3 * std::mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Fault>() == 4 * mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Route>() == 1);
const _: () = assert!(std::mem::size_of::<Layout>() == 1);
const _: () = assert!(std::mem::size_of::<Reply>() == 1);
const _: () = assert!(std::mem::size_of::<Encode>() <= 3 * std::mem::size_of::<usize>());
