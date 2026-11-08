use o3::buffer;
use o3::buffer::hold::scope;

use crate::failure;
use crate::failure::wire::Fault;
use crate::protocol::method::{self, basic, channel, confirm, queue};
use crate::protocol::wire::text::fields;

pub(crate) enum Event<'input, 'd> {
    Done(method::Kind),
    Open(channel::OpenOk),
    Flow(channel::Flow),
    Close(channel::Close<'d>),
    CloseOk(channel::CloseOk),
    QueueDeclare(queue::View<'input>),
    QueuePurge(queue::PurgeOk),
    QueueDelete(queue::DeleteOk),
    Consume(basic::View<'input>),
    Cancel(basic::Cancel<'input>),
    CancelOk(basic::CancelOk<'d>),
    Return(basic::Return<'input>),
    Deliver(basic::Deliver<'input>),
    Ack(basic::Ack),
    Nack(basic::Nack),
    Confirm(confirm::SelectOk),
}

impl<'input, 'd> Event<'input, 'd> {
    pub(crate) fn decode<R: buffer::Cursor + 'input>(
        args: &scope::Deferred<'input, 'd, R>,
        kind: method::Kind,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<Self, failure::Error> {
        match kind {
            method::Kind::ExchangeDeclare
            | method::Kind::ExchangeDelete
            | method::Kind::ExchangeBind
            | method::Kind::ExchangeUnbind
            | method::Kind::QueueBind
            | method::Kind::QueueUnbind
            | method::Kind::Qos
            | method::Kind::Recover
            | method::Kind::TxSelect
            | method::Kind::TxCommit
            | method::Kind::TxRollback => {
                fields::Args::new(args).empty().map(|()| Self::Done(kind))
            }
            method::Kind::Open => fields::Args::new(args)
                .decode(channel::OpenOk::read)
                .map(Self::Open),
            method::Kind::Flow => fields::Args::new(args)
                .decode(channel::Flow::read)
                .map(Self::Flow),
            method::Kind::FlowOk => unreachable!("flowok"),
            method::Kind::Close => channel::Close::retain(args, budget).map(Self::Close),
            method::Kind::CloseOk => fields::Args::new(args)
                .empty()
                .map(|()| Self::CloseOk(channel::CloseOk)),
            method::Kind::QueueDeclare => queue::View::read(args).map(Self::QueueDeclare),
            method::Kind::QueuePurge => fields::Args::new(args)
                .decode(queue::PurgeOk::read)
                .map(Self::QueuePurge),
            method::Kind::QueueDelete => fields::Args::new(args)
                .decode(queue::DeleteOk::read)
                .map(Self::QueueDelete),
            method::Kind::Consume => basic::View::read(args).map(Self::Consume),
            method::Kind::Cancel => fields::Args::new(args)
                .decode(basic::Cancel::read)
                .map(Self::Cancel),
            method::Kind::CancelOk => basic::CancelOk::read(args, budget).map(Self::CancelOk),
            method::Kind::Return => basic::Return::read(args).map(Self::Return),
            method::Kind::Deliver => basic::Deliver::read(args).map(Self::Deliver),
            actual @ (method::Kind::Get | method::Kind::GetEmpty) => {
                Err(failure::Error::Protocol(Fault::GetUnsolicited { actual }))
            }
            method::Kind::Ack => fields::Args::new(args)
                .decode(basic::Ack::read)
                .map(Self::Ack),
            method::Kind::Nack => fields::Args::new(args)
                .decode(basic::Nack::read)
                .map(Self::Nack),
            method::Kind::Confirm => fields::Args::new(args)
                .empty()
                .map(|()| Self::Confirm(confirm::SelectOk)),
        }
    }
}
