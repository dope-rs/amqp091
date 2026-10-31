use o3::buffer;
use o3::buffer::hold::scope;

use crate::failure;
use crate::failure::wire::Fault;
use crate::protocol::method::{self, basic, channel, confirm, exchange, queue, tx};
use crate::protocol::wire::text::fields;

pub(crate) enum Event<'input, 'd> {
    Open(channel::OpenOk),
    Flow(channel::Flow),
    FlowOk(channel::FlowOk),
    Close(channel::Close<'d>),
    CloseOk(channel::CloseOk),
    ExchangeDeclare(exchange::DeclareOk),
    ExchangeDelete(exchange::DeleteOk),
    ExchangeBind(exchange::BindOk),
    ExchangeUnbind(exchange::UnbindOk),
    QueueDeclare(queue::View<'input>),
    QueueBind(queue::BindOk),
    QueueUnbind(queue::UnbindOk),
    QueuePurge(queue::PurgeOk),
    QueueDelete(queue::DeleteOk),
    Qos(basic::QosOk),
    Consume(basic::View<'input>),
    Cancel(basic::Cancel<'input>),
    CancelOk(basic::CancelOk<'d>),
    Return(basic::Return<'input>),
    Deliver(basic::Deliver<'input>),
    Ack(basic::Ack),
    Nack(basic::Nack),
    Recover(basic::RecoverOk),
    Confirm(confirm::SelectOk),
    TxSelect(tx::SelectOk),
    TxCommit(tx::CommitOk),
    TxRollback(tx::RollbackOk),
}

impl<'input, 'd> Event<'input, 'd> {
    pub(crate) fn decode<R: buffer::Cursor + 'input>(
        args: &scope::Deferred<'input, 'd, R>,
        kind: method::Kind,
        budget: &buffer::resident::Budget<'d>,
    ) -> Result<Self, failure::Error> {
        match kind {
            method::Kind::Open => fields::Args::new(args)
                .decode(channel::OpenOk::read)
                .map(Self::Open),
            method::Kind::Flow => fields::Args::new(args)
                .decode(channel::Flow::read)
                .map(Self::Flow),
            method::Kind::FlowOk => fields::Args::new(args)
                .decode(channel::FlowOk::read)
                .map(Self::FlowOk),
            method::Kind::Close => channel::Close::retain(args, budget).map(Self::Close),
            method::Kind::CloseOk => fields::Args::new(args)
                .empty()
                .map(|()| Self::CloseOk(channel::CloseOk)),
            method::Kind::ExchangeDeclare => fields::Args::new(args)
                .empty()
                .map(|()| Self::ExchangeDeclare(exchange::DeclareOk)),
            method::Kind::ExchangeDelete => fields::Args::new(args)
                .empty()
                .map(|()| Self::ExchangeDelete(exchange::DeleteOk)),
            method::Kind::ExchangeBind => fields::Args::new(args)
                .empty()
                .map(|()| Self::ExchangeBind(exchange::BindOk)),
            method::Kind::ExchangeUnbind => fields::Args::new(args)
                .empty()
                .map(|()| Self::ExchangeUnbind(exchange::UnbindOk)),
            method::Kind::QueueDeclare => queue::View::read(args).map(Self::QueueDeclare),
            method::Kind::QueueBind => fields::Args::new(args)
                .empty()
                .map(|()| Self::QueueBind(queue::BindOk)),
            method::Kind::QueueUnbind => fields::Args::new(args)
                .empty()
                .map(|()| Self::QueueUnbind(queue::UnbindOk)),
            method::Kind::QueuePurge => fields::Args::new(args)
                .decode(queue::PurgeOk::read)
                .map(Self::QueuePurge),
            method::Kind::QueueDelete => fields::Args::new(args)
                .decode(queue::DeleteOk::read)
                .map(Self::QueueDelete),
            method::Kind::Qos => fields::Args::new(args)
                .empty()
                .map(|()| Self::Qos(basic::QosOk)),
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
            method::Kind::Recover => fields::Args::new(args)
                .empty()
                .map(|()| Self::Recover(basic::RecoverOk)),
            method::Kind::Confirm => fields::Args::new(args)
                .empty()
                .map(|()| Self::Confirm(confirm::SelectOk)),
            method::Kind::TxSelect => fields::Args::new(args)
                .empty()
                .map(|()| Self::TxSelect(tx::SelectOk)),
            method::Kind::TxCommit => fields::Args::new(args)
                .empty()
                .map(|()| Self::TxCommit(tx::CommitOk)),
            method::Kind::TxRollback => fields::Args::new(args)
                .empty()
                .map(|()| Self::TxRollback(tx::RollbackOk)),
        }
    }
}
