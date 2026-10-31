use std::num;

use dope::core::Errno;
use dope::core::driver::schedule::ready::wake::{fanout, queue, table};
use dope::net::link::egress::reserve::error;
use dope::net::link::egress::{self};
use dope::net::link::pool::change::opening;
use o3::alloc;
use o3::buffer::resident::AllocError;
use o3::buffer::write::consume::scope;
use o3::buffer::{hold, storage};
use o3::depot::arena::slab::key;
use o3::depot::queue::round;
use o3::mem::rc;

use crate::failure::wire::{field, turn};
use crate::failure::{self, wire};
use crate::protocol::wire::text::resident::{Exact, Short};
use crate::protocol::wire::text::short;
use crate::protocol::wire::{frame, reader};

#[derive(Debug)]
pub enum Error<Text = short::Buf> {
    Opening(opening::Exhaustion),
    Input(scope::Error),
    Retain(rc::Error),
    Allocation {
        target: wire::Allocation,
        error: alloc::Error,
    },
    Exhausted {
        target: wire::Allocation,
        error: alloc::Exhausted,
    },
    Storage {
        target: wire::Allocation,
        error: storage::AllocError,
    },
    Resident {
        target: wire::Allocation,
        error: wire::Resident,
    },
    Hold {
        target: wire::Allocation,
        error: hold::Error,
    },
    Method(key::Error<turn::Request>),
    MethodIdentity(num::NonZero<u64>),
    Closed,
    Polled,
    Disconnected(failure::Reason),
    Receive {
        errno: Errno,
    },
    Protocol(wire::Fault),
    Field(field::Fault),
    Decode(reader::Fault),
    UnexpectedFlowOk {
        active: bool,
    },
    Server {
        reply_code: u16,
        reply_text: Text,
        class_id: u16,
        method_id: u16,
    },
    QueueName {
        expected: Text,
        actual: Text,
    },
    ConsumerTag {
        expected: Text,
        actual: Text,
    },
    ConsumerCollision {
        error: turn::Clash,
        tag: Text,
    },
    UnsupportedMechanism,
    UnsupportedLocale,
    Version {
        major: u8,
        minor: u8,
    },
    FrameTooLarge {
        size: u64,
        max: u32,
    },
    MessageTooLarge {
        size: u64,
        max: usize,
    },
    InvalidConfig(wire::Config),
    Capacity(wire::Capacity),
    Egress(egress::Rejection),
    Reserve(error::Lane),
    Encoding(wire::Encode),
    UnknownClass(u16),
    UnknownMethod {
        class_id: u16,
        method_id: u16,
    },
    Frame(frame::Fault),
    ConnectionBlocked(Text),
    ChannelFlowPaused,
    ChannelInUse(u16),
    ChannelCapacity {
        capacity: u16,
    },
    Queue(queue::Error),
    Cancellation(round::Bounds),
    WaitCancel(round::Bounds),
    WaitRoute(round::Bounds),
    WaitRegister(round::Bounds),
    Fanout(fanout::Error),
    Table(table::Error),
    ServerNamedQueueNotExclusive,
}

impl<Text> Error<Text> {
    pub(crate) const fn storage(target: wire::Allocation, error: storage::AllocError) -> Self {
        Self::Storage { target, error }
    }

    pub(crate) fn resident(target: wire::Allocation, error: AllocError) -> Self {
        match error {
            AllocError::Capacity(error) => {
                let (actual, max) = error.into_parts();
                Self::Resident {
                    target,
                    error: wire::Resident::Capacity {
                        attempted: actual,
                        capacity: max,
                    },
                }
            }
            AllocError::Budget(error) => {
                let (need, free) = error.into_parts();
                Self::Resident {
                    target,
                    error: wire::Resident::Budget {
                        required: need,
                        available: free,
                    },
                }
            }
            AllocError::Layout(error) => Self::Resident {
                target,
                error: wire::Resident::Layout(error),
            },
            AllocError::Exhausted(error) => Self::Exhausted { target, error },
            AllocError::Retain(error) => Self::Resident {
                target,
                error: wire::Resident::Retain(error),
            },
        }
    }

    fn map_text<Mapped>(self, mut map: impl FnMut(Text) -> Mapped) -> Error<Mapped> {
        match self {
            Self::Opening(error) => Error::Opening(error),
            Self::Input(error) => Error::Input(error),
            Self::Retain(error) => Error::Retain(error),
            Self::Allocation { target, error } => Error::Allocation { target, error },
            Self::Exhausted { target, error } => Error::Exhausted { target, error },
            Self::Storage { target, error } => Error::Storage { target, error },
            Self::Resident { target, error } => Error::Resident { target, error },
            Self::Hold { target, error } => Error::Hold { target, error },
            Self::Method(error) => Error::Method(error),
            Self::MethodIdentity(id) => Error::MethodIdentity(id),
            Self::Closed => Error::Closed,
            Self::Polled => Error::Polled,
            Self::Disconnected(reason) => Error::Disconnected(reason),
            Self::Receive { errno } => Error::Receive { errno },
            Self::Protocol(fault) => Error::Protocol(fault),
            Self::Field(fault) => Error::Field(fault),
            Self::Decode(error) => Error::Decode(error),
            Self::UnexpectedFlowOk { active } => Error::UnexpectedFlowOk { active },
            Self::Server {
                reply_code: code,
                reply_text: text,
                class_id: class,
                method_id: method,
            } => Error::Server {
                reply_code: code,
                reply_text: map(text),
                class_id: class,
                method_id: method,
            },
            Self::QueueName { expected, actual } => Error::QueueName {
                expected: map(expected),
                actual: map(actual),
            },
            Self::ConsumerTag { expected, actual } => Error::ConsumerTag {
                expected: map(expected),
                actual: map(actual),
            },
            Self::ConsumerCollision { error, tag } => Error::ConsumerCollision {
                error,
                tag: map(tag),
            },
            Self::UnsupportedMechanism => Error::UnsupportedMechanism,
            Self::UnsupportedLocale => Error::UnsupportedLocale,
            Self::Version { major, minor } => Error::Version { major, minor },
            Self::FrameTooLarge { size, max } => Error::FrameTooLarge { size, max },
            Self::MessageTooLarge { size, max } => Error::MessageTooLarge { size, max },
            Self::InvalidConfig(error) => Error::InvalidConfig(error),
            Self::Capacity(error) => Error::Capacity(error),
            Self::Egress(error) => Error::Egress(error),
            Self::Reserve(error) => Error::Reserve(error),
            Self::Encoding(error) => Error::Encoding(error),
            Self::UnknownClass(class) => Error::UnknownClass(class),
            Self::UnknownMethod {
                class_id: class,
                method_id: method,
            } => Error::UnknownMethod {
                class_id: class,
                method_id: method,
            },
            Self::Frame(fault) => Error::Frame(fault),
            Self::ConnectionBlocked(reason) => Error::ConnectionBlocked(map(reason)),
            Self::ChannelFlowPaused => Error::ChannelFlowPaused,
            Self::ChannelInUse(id) => Error::ChannelInUse(id),
            Self::ChannelCapacity { capacity } => Error::ChannelCapacity { capacity },
            Self::Queue(error) => Error::Queue(error),
            Self::Cancellation(error) => Error::Cancellation(error),
            Self::WaitCancel(error) => Error::WaitCancel(error),
            Self::WaitRoute(error) => Error::WaitRoute(error),
            Self::WaitRegister(error) => Error::WaitRegister(error),
            Self::Fanout(error) => Error::Fanout(error),
            Self::Table(error) => Error::Table(error),
            Self::ServerNamedQueueNotExclusive => Error::ServerNamedQueueNotExclusive,
        }
    }
}

impl<'b, 'd> From<Error> for Error<Short<'b, 'd>> {
    fn from(error: Error) -> Self {
        error.map_text(|value| Exact::from_owned(value).into())
    }
}

const ACCESS_REFUSED: u16 = 403;
const NOT_FOUND: u16 = 404;
const NOT_ALLOWED: u16 = 530;

impl<Text> Error<Text> {
    pub(crate) fn close_reply(&self) -> Option<wire::Reply> {
        match self {
            Self::FrameTooLarge { .. } | Self::Frame(frame::Fault::FrameTooLarge { .. }) => {
                Some(wire::Reply::Frame)
            }
            Self::UnknownClass(_)
            | Self::UnknownMethod { .. }
            | Self::Protocol(wire::Fault::ConnMethod { .. }) => Some(wire::Reply::Command),
            Self::Protocol(wire::Fault::ContentInterrupted) => Some(wire::Reply::Unexpected),
            Self::Protocol(
                wire::Fault::ChanMethod { .. }
                | wire::Fault::ChanResponse { .. }
                | wire::Fault::Response { .. }
                | wire::Fault::ConsumerKey { .. }
                | wire::Fault::ConsumerIndex { .. }
                | wire::Fault::ConsumerPhase { .. }
                | wire::Fault::MethodState { .. }
                | wire::Fault::MethodOrder { .. }
                | wire::Fault::MethodControl { .. }
                | wire::Fault::FlightMethod { .. }
                | wire::Fault::FlightState { .. }
                | wire::Fault::WorkReply { .. }
                | wire::Fault::QueueIdentity { .. }
                | wire::Fault::TopologyIdentity { .. }
                | wire::Fault::TopologyQueue { .. }
                | wire::Fault::ReplayPhase { .. }
                | wire::Fault::CloseClosed
                | wire::Fault::ChannelNotReady { .. }
                | wire::Fault::ChannelUnknown { .. }
                | wire::Fault::ControlRoute { .. }
                | wire::Fault::ControlMethod { .. }
                | wire::Fault::ConnectionChannel { .. }
                | wire::Fault::ChannelBounds(_)
                | wire::Fault::ChannelZero
                | wire::Fault::ChannelMax { .. },
            ) => Some(wire::Reply::Channel),
            Self::QueueName { .. } | Self::ConsumerTag { .. } | Self::ConsumerCollision { .. } => {
                Some(wire::Reply::Channel)
            }
            Self::Frame(_) => None,
            Self::Protocol(_)
            | Self::Field(_)
            | Self::Decode(_)
            | Self::UnexpectedFlowOk { .. } => Some(wire::Reply::Syntax),
            _ => None,
        }
    }

    pub fn server_code(&self) -> Option<u16> {
        match self {
            Self::Server {
                reply_code: code, ..
            } => Some(*code),
            _ => None,
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.server_code() == Some(NOT_FOUND)
    }

    pub fn is_refused(&self) -> bool {
        self.server_code() == Some(ACCESS_REFUSED)
    }

    pub fn is_denied(&self) -> bool {
        self.server_code() == Some(NOT_ALLOWED)
    }
}
