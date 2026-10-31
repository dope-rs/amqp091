use std::marker;

use crate::failure;
use crate::failure::wire::Fault;
use crate::message::ack;
use crate::protocol::method::{basic, queue};
use crate::protocol::wire::frame;

pub(crate) mod class;
pub mod method;
pub(crate) mod replays;
pub(crate) mod topology;
pub mod wire;

/// Capacity of the largest control-lane frame, `Channel.Close`.
pub(crate) const CONTROL_FRAME_CAPACITY: usize =
    frame::OVERHEAD + 4 + 2 + 1 + u8::MAX as usize + 2 + 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Confirmation {
    Ack,
    Nack { requeue: bool },
}

pub enum Response<'b, 'd, Consumer> {
    Empty(method::Kind),
    QueueDeclareOk(queue::DeclareOk<'b, 'd>),
    QueuePurgeOk(queue::PurgeOk),
    QueueDeleteOk(queue::DeleteOk),
    BasicConsumeOk(basic::ConsumeOk<'b, 'd>, Consumer),
    BasicConsumeReplayOk,
    BasicCancelOk(basic::CancelOk<'d>),
}

pub enum Registration<'b, 'd, Consumer> {
    Registered(basic::ConsumeOk<'b, 'd>, Consumer),
    Replayed,
}

impl<'b, 'd, Consumer> method::Complete<'b, 'd, Consumer> for Registration<'b, 'd, Consumer> {
    type Reply = basic::ConsumeReply;
}

impl<'b, 'd, Consumer, R: method::Unit> From<R> for Response<'b, 'd, Consumer> {
    fn from(reply: R) -> Self {
        Self::Empty(reply.into_kind())
    }
}

impl<'b, 'd, Consumer> From<queue::DeclareOk<'b, 'd>> for Response<'b, 'd, Consumer> {
    fn from(reply: queue::DeclareOk<'b, 'd>) -> Self {
        Self::QueueDeclareOk(reply)
    }
}

impl<'b, 'd, Consumer> From<queue::PurgeOk> for Response<'b, 'd, Consumer> {
    fn from(reply: queue::PurgeOk) -> Self {
        Self::QueuePurgeOk(reply)
    }
}

impl<'b, 'd, Consumer> From<queue::DeleteOk> for Response<'b, 'd, Consumer> {
    fn from(reply: queue::DeleteOk) -> Self {
        Self::QueueDeleteOk(reply)
    }
}

impl<'b, 'd, Consumer> From<Registration<'b, 'd, Consumer>> for Response<'b, 'd, Consumer> {
    fn from(done: Registration<'b, 'd, Consumer>) -> Self {
        match done {
            Registration::Registered(reply, key) => Self::BasicConsumeOk(reply, key),
            Registration::Replayed => Self::BasicConsumeReplayOk,
        }
    }
}

impl<'b, 'd, Consumer> From<basic::CancelOk<'d>> for Response<'b, 'd, Consumer> {
    fn from(reply: basic::CancelOk<'d>) -> Self {
        Self::BasicCancelOk(reply)
    }
}

/// A consumer identity bound to the acknowledgement mode sent on the wire.
pub struct ConsumerKey<Consumer, M = ack::Manual> {
    inner: Consumer,
    mode: marker::PhantomData<fn() -> M>,
}

impl<Consumer, M> ConsumerKey<Consumer, M> {
    fn new(inner: Consumer) -> Self {
        Self {
            inner,
            mode: marker::PhantomData,
        }
    }

    pub(crate) const fn inner(&self) -> &Consumer {
        &self.inner
    }

    pub(crate) fn into_inner(self) -> Consumer {
        self.inner
    }
}

pub struct Consumer<'b, 'd, Key, M = ack::Manual> {
    pub method: basic::ConsumeOk<'b, 'd>,
    pub key: ConsumerKey<Key, M>,
}

impl<'b, 'd, Key, M> method::Finish<Registration<'b, 'd, Key>> for Consumer<'b, 'd, Key, M> {
    fn finish(input: Registration<'b, 'd, Key>) -> Result<Self, failure::Error> {
        use failure::wire::Consumer;

        match input {
            Registration::Registered(method, key) => Ok(Self {
                method,
                key: ConsumerKey::new(key),
            }),
            Registration::Replayed => Err(failure::Error::Protocol(Fault::Consumer {
                expected: Consumer::New,
                actual: Consumer::Replay,
            })),
        }
    }
}

impl<'b, 'd, Consumer> Response<'b, 'd, Consumer> {
    pub(crate) const fn kind(&self) -> method::Kind {
        match self {
            Self::Empty(kind) => *kind,
            Self::QueueDeclareOk(_) => method::Kind::QueueDeclare,
            Self::QueuePurgeOk(_) => method::Kind::QueuePurge,
            Self::QueueDeleteOk(_) => method::Kind::QueueDelete,
            Self::BasicConsumeOk(_, _) | Self::BasicConsumeReplayOk => method::Kind::Consume,
            Self::BasicCancelOk(_) => method::Kind::CancelOk,
        }
    }

    pub(crate) fn try_map<Mapped>(
        self,
        map: impl FnOnce(Consumer) -> Result<Mapped, failure::Error>,
    ) -> Result<Response<'b, 'd, Mapped>, failure::Error> {
        Ok(match self {
            Self::Empty(kind) => Response::Empty(kind),
            Self::QueueDeclareOk(value) => Response::QueueDeclareOk(value),
            Self::QueuePurgeOk(value) => Response::QueuePurgeOk(value),
            Self::QueueDeleteOk(value) => Response::QueueDeleteOk(value),
            Self::BasicConsumeOk(value, key) => Response::BasicConsumeOk(value, map(key)?),
            Self::BasicConsumeReplayOk => Response::BasicConsumeReplayOk,
            Self::BasicCancelOk(value) => Response::BasicCancelOk(value),
        })
    }
}

#[derive(Default, Clone, Copy)]
pub struct Negotiated {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}
