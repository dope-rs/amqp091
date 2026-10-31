use crate::protocol::wire::Write;
pub mod basic;
pub mod channel;
pub mod confirm;
pub mod conn;
pub mod exchange;
pub mod queue;
pub mod tx;

use std::mem;

use crate::failure;
use crate::failure::wire::Fault;
use crate::protocol::wire::{self, frame, outputs, schema};
use crate::protocol::{self, class};
use crate::state::work::requests;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Id(u32);

impl Id {
    pub(crate) const SIZE: usize = mem::size_of::<u32>();

    pub(crate) const fn new(class: class::Class, method: u16) -> Self {
        Self(((class.get() as u32) << 16) | method as u32)
    }

    pub const fn class(self) -> u16 {
        (self.0 >> 16) as u16
    }

    pub const fn method(self) -> u16 {
        self.0 as u16
    }

    pub(crate) const fn bytes(self) -> [u8; 4] {
        self.0.to_be_bytes()
    }

    pub(crate) const fn from_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_be_bytes(bytes))
    }

    pub(crate) const fn is_conn(self) -> bool {
        self.class() == class::CONNECTION.get()
    }
}

pub trait Spec {
    const ID: Id;
}

pub trait Encode: Spec {
    fn write<S: Write + AsMut<[u8]> + ?Sized>(&self, out: &mut S, id: u16);
}

pub(crate) trait Args: Spec {
    fn write_args<S: wire::Put>(&self, out: &mut S);
}

impl<M: Args> Encode for M {
    fn write<S: Write + AsMut<[u8]> + ?Sized>(&self, out: &mut S, id: u16) {
        frame::Encoder::new(out).method_with(id, Self::ID.bytes(), |out| {
            let mut plain = outputs::Plain::new(out);
            self.write_args(&mut plain);
        });
    }
}

pub(crate) trait Record<L: schema::Layout>: Encode {
    fn record<'a>(&self, out: outputs::Fields<'a, L>) -> outputs::Fields<'a, L, L::Proof>;
}

impl<M: Args> Record<schema::Empty> for M {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, schema::Empty>,
    ) -> outputs::Fields<'a, schema::Empty> {
        self.write_args(&mut out);
        out
    }
}

#[doc(hidden)]
pub trait Reply: Spec {
    const KIND: Kind;
    type Output<'b, 'd, Consumer>;
    type Value<'b, 'd, Consumer>;

    fn project<'b, 'd, Consumer>(
        reply: protocol::Response<'b, 'd, Consumer>,
    ) -> Result<Self::Output<'b, 'd, Consumer>, failure::Error>;
}

pub(crate) trait Unit: Spec + Sized {
    const KIND: Kind;

    fn into_kind(self) -> Kind {
        Self::KIND
    }
}

impl<R: Unit> Reply for R {
    const KIND: Kind = R::KIND;
    type Output<'b, 'd, Consumer> = ();
    type Value<'b, 'd, Consumer> = Self;

    fn project<'b, 'd, Consumer>(
        reply: protocol::Response<'b, 'd, Consumer>,
    ) -> Result<(), failure::Error> {
        let actual = reply.kind();
        match reply {
            protocol::Response::Empty(kind) if kind == R::KIND => Ok(()),
            _ => Err(failure::Error::Protocol(Fault::Response {
                expected: R::KIND,
                actual,
            })),
        }
    }
}

pub(crate) trait Complete<'b, 'd, Consumer>:
    Into<<Self::Reply as Reply>::Value<'b, 'd, Consumer>>
{
    type Reply: Reply;
}

impl<'b, 'd, Consumer, R> Complete<'b, 'd, Consumer> for R
where
    R: Reply,
    R::Value<'b, 'd, Consumer>: From<R>,
{
    type Reply = R;
}

pub trait Request: Encode {
    type Response: Reply;
}

#[doc(hidden)]
pub trait Finish<Input>: Sized {
    fn finish(input: Input) -> Result<Self, failure::Error>;
}

impl<T> Finish<T> for T {
    fn finish(input: T) -> Result<Self, failure::Error> {
        Ok(input)
    }
}

impl<'b, 'd, C> Finish<protocol::Registration<'b, 'd, C>> for () {
    fn finish(input: protocol::Registration<'b, 'd, C>) -> Result<Self, failure::Error> {
        use failure::wire::Consumer;

        match input {
            protocol::Registration::Replayed => Ok(()),
            protocol::Registration::Registered(_, _) => {
                Err(failure::Error::Protocol(Fault::Consumer {
                    expected: Consumer::Replay,
                    actual: Consumer::New,
                }))
            }
        }
    }
}

pub trait Op<'d>: Request + requests::Source<'d> {
    type Output<'b, Consumer>: Finish<<Self::Response as Reply>::Output<'b, 'd, Consumer>>;

    #[doc(hidden)]
    fn finish<'b, Consumer>(
        input: <Self::Response as Reply>::Output<'b, 'd, Consumer>,
    ) -> Result<<Self as Op<'d>>::Output<'b, Consumer>, failure::Error> {
        Finish::finish(input)
    }
}

#[doc(hidden)]
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Open,
    Flow,
    FlowOk,
    Close,
    CloseOk,
    ExchangeDeclare,
    ExchangeDelete,
    ExchangeBind,
    ExchangeUnbind,
    QueueDeclare,
    QueueBind,
    QueueUnbind,
    QueuePurge,
    QueueDelete,
    Qos,
    Consume,
    Cancel,
    CancelOk,
    Return,
    Deliver,
    Get,
    GetEmpty,
    Ack,
    Nack,
    Recover,
    Confirm,
    TxSelect,
    TxCommit,
    TxRollback,
}

impl Kind {
    pub(crate) fn identify(id: Id) -> Result<Self, failure::Error> {
        let method = id.method();
        let kind = match id.class() {
            value if value == class::CHANNEL.get() => match method {
                value if value == channel::OPEN_OK.method() => Self::Open,
                value if value == channel::FLOW.method() => Self::Flow,
                value if value == channel::FLOW_OK.method() => Self::FlowOk,
                value if value == channel::CLOSE.method() => Self::Close,
                value if value == channel::CLOSE_OK.method() => Self::CloseOk,
                _ => return Err(Self::unknown(id)),
            },
            value if value == class::EXCHANGE.get() => match method {
                value if value == exchange::DECLARE_OK.method() => Self::ExchangeDeclare,
                value if value == exchange::DELETE_OK.method() => Self::ExchangeDelete,
                value if value == exchange::BIND_OK.method() => Self::ExchangeBind,
                value if value == exchange::UNBIND_OK.method() => Self::ExchangeUnbind,
                _ => return Err(Self::unknown(id)),
            },
            value if value == class::QUEUE.get() => match method {
                value if value == queue::DECLARE_OK.method() => Self::QueueDeclare,
                value if value == queue::BIND_OK.method() => Self::QueueBind,
                value if value == queue::UNBIND_OK.method() => Self::QueueUnbind,
                value if value == queue::PURGE_OK.method() => Self::QueuePurge,
                value if value == queue::DELETE_OK.method() => Self::QueueDelete,
                _ => return Err(Self::unknown(id)),
            },
            value if value == class::BASIC.get() => match method {
                value if value == basic::QOS_OK.method() => Self::Qos,
                value if value == basic::CONSUME_OK.method() => Self::Consume,
                value if value == basic::CANCEL.method() => Self::Cancel,
                value if value == basic::CANCEL_OK.method() => Self::CancelOk,
                value if value == basic::RETURN.method() => Self::Return,
                value if value == basic::DELIVER.method() => Self::Deliver,
                value if value == basic::GET_OK.method() => Self::Get,
                value if value == basic::GET_EMPTY.method() => Self::GetEmpty,
                value if value == basic::ACK.method() => Self::Ack,
                value if value == basic::NACK.method() => Self::Nack,
                value if value == basic::RECOVER_OK.method() => Self::Recover,
                _ => return Err(Self::unknown(id)),
            },
            value if value == class::CONFIRM.get() => match method {
                value if value == confirm::SELECT_OK.method() => Self::Confirm,
                _ => return Err(Self::unknown(id)),
            },
            value if value == class::TX.get() => match method {
                value if value == tx::SELECT_OK.method() => Self::TxSelect,
                value if value == tx::COMMIT_OK.method() => Self::TxCommit,
                value if value == tx::ROLLBACK_OK.method() => Self::TxRollback,
                _ => return Err(Self::unknown(id)),
            },
            _ => return Err(failure::Error::UnknownClass(id.class())),
        };
        Ok(kind)
    }

    fn unknown(id: Id) -> failure::Error {
        failure::Error::UnknownMethod {
            class_id: id.class(),
            method_id: id.method(),
        }
    }
}

const _: () = {
    assert!(size_of::<Id>() == size_of::<u32>());
    assert!(size_of::<Kind>() == 1);
    assert!(size_of::<Option<Kind>>() == 1);
};
