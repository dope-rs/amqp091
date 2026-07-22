pub mod basic;
pub mod channel;
pub mod confirm;
pub mod connection;
pub mod core;
pub mod exchange;
pub mod queue;
pub mod tx;
use o3::buffer::Shared;

use crate::error::Error;
use crate::method::basic::{
    Ack, Cancel, CancelOk, ConsumeOk, Deliver, GetEmpty, GetOk, Nack, QosOk, RecoverOk, Return,
};
use crate::method::channel::{Flow, FlowOk};
use crate::method::confirm::SelectOk;
use crate::method::connection::{Blocked, Close, CloseOk, OpenOk, Secure, Start, Tune, Unblocked};
use crate::method::core::{Basic, Channel, ClassId, Confirm, Connection, Exchange, Queue, Tx};
use crate::method::exchange::{BindOk, DeclareOk, DeleteOk, UnbindOk};
use crate::method::queue::PurgeOk;
use crate::method::tx::{CommitOk, RollbackOk};
use crate::wire::reader::Reader;

#[derive(Debug)]
pub enum Server {
    ConnectionStart(connection::Start),
    ConnectionSecure(connection::Secure),
    ConnectionTune(connection::Tune),
    ConnectionOpenOk(connection::OpenOk),
    ConnectionClose(connection::Close),
    ConnectionCloseOk(connection::CloseOk),
    ConnectionBlocked(connection::Blocked),
    ConnectionUnblocked(connection::Unblocked),
    ChannelOpenOk(channel::OpenOk),
    ChannelFlow(channel::Flow),
    ChannelFlowOk(channel::FlowOk),
    ChannelClose(channel::Close),
    ChannelCloseOk(channel::CloseOk),
    ExchangeDeclareOk(exchange::DeclareOk),
    ExchangeDeleteOk(exchange::DeleteOk),
    ExchangeBindOk(exchange::BindOk),
    ExchangeUnbindOk(exchange::UnbindOk),
    QueueDeclareOk(queue::DeclareOk),
    QueueBindOk(queue::BindOk),
    QueueUnbindOk(queue::UnbindOk),
    QueuePurgeOk(queue::PurgeOk),
    QueueDeleteOk(queue::DeleteOk),
    BasicQosOk(basic::QosOk),
    BasicConsumeOk(basic::ConsumeOk),
    BasicCancel(basic::Cancel),
    BasicCancelOk(basic::CancelOk),
    BasicReturn(basic::Return),
    BasicDeliver(basic::Deliver),
    BasicGetOk(basic::GetOk),
    BasicGetEmpty(basic::GetEmpty),
    BasicAck(basic::Ack),
    BasicNack(basic::Nack),
    BasicRecoverOk(basic::RecoverOk),
    ConfirmSelectOk(confirm::SelectOk),
    TxSelectOk(tx::SelectOk),
    TxCommitOk(tx::CommitOk),
    TxRollbackOk(tx::RollbackOk),
}

impl Server {
    pub fn parse(payload: Shared) -> Result<Self, Error> {
        let mut r = Reader::new(&payload);
        let class_id = r.take_u16()?;
        let method_id = r.take_u16()?;
        match class_id {
            ClassId::CONNECTION => Self::parse_connection(method_id, &mut r),
            ClassId::CHANNEL => Self::parse_channel(method_id, &mut r),
            ClassId::EXCHANGE => Self::parse_exchange(method_id, &mut r),
            ClassId::QUEUE => Self::parse_queue(method_id, &mut r),
            ClassId::BASIC => Self::parse_basic(method_id, &mut r, &payload),
            ClassId::CONFIRM => Self::parse_confirm(method_id, &mut r),
            ClassId::TX => Self::parse_tx(method_id, &mut r),
            other => Err(Error::UnknownClass(other)),
        }
    }

    fn parse_connection(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match method_id {
            Connection::START => Self::ConnectionStart(Start::read(r)?),
            Connection::SECURE => Self::ConnectionSecure(Secure::read(r)?),
            Connection::TUNE => Self::ConnectionTune(Tune::read(r)?),
            Connection::OPEN_OK => Self::ConnectionOpenOk(OpenOk::read(r)?),
            Connection::CLOSE => Self::ConnectionClose(Close::read(r)?),
            Connection::CLOSE_OK => Self::ConnectionCloseOk(CloseOk::read(r)?),
            Connection::BLOCKED => Self::ConnectionBlocked(Blocked::read(r)?),
            Connection::UNBLOCKED => Self::ConnectionUnblocked(Unblocked::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::CONNECTION,
                    method_id: id,
                });
            }
        })
    }

    fn parse_channel(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        use channel::{Close, CloseOk, OpenOk};

        Ok(match method_id {
            Channel::OPEN_OK => Self::ChannelOpenOk(OpenOk::read(r)?),
            Channel::FLOW => Self::ChannelFlow(Flow::read(r)?),
            Channel::FLOW_OK => Self::ChannelFlowOk(FlowOk::read(r)?),
            Channel::CLOSE => Self::ChannelClose(Close::read(r)?),
            Channel::CLOSE_OK => Self::ChannelCloseOk(CloseOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::CHANNEL,
                    method_id: id,
                });
            }
        })
    }

    fn parse_exchange(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match method_id {
            Exchange::DECLARE_OK => Self::ExchangeDeclareOk(DeclareOk::read(r)?),
            Exchange::DELETE_OK => Self::ExchangeDeleteOk(DeleteOk::read(r)?),
            Exchange::BIND_OK => Self::ExchangeBindOk(BindOk::read(r)?),
            Exchange::UNBIND_OK => Self::ExchangeUnbindOk(UnbindOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::EXCHANGE,
                    method_id: id,
                });
            }
        })
    }

    fn parse_queue(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        use queue::{BindOk, DeclareOk, DeleteOk, UnbindOk};

        Ok(match method_id {
            Queue::DECLARE_OK => Self::QueueDeclareOk(DeclareOk::read(r)?),
            Queue::BIND_OK => Self::QueueBindOk(BindOk::read(r)?),
            Queue::UNBIND_OK => Self::QueueUnbindOk(UnbindOk::read(r)?),
            Queue::PURGE_OK => Self::QueuePurgeOk(PurgeOk::read(r)?),
            Queue::DELETE_OK => Self::QueueDeleteOk(DeleteOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::QUEUE,
                    method_id: id,
                });
            }
        })
    }

    fn parse_basic(method_id: u16, r: &mut Reader<'_>, payload: &Shared) -> Result<Self, Error> {
        Ok(match method_id {
            Basic::QOS_OK => Self::BasicQosOk(QosOk::read(r)?),
            Basic::CONSUME_OK => Self::BasicConsumeOk(ConsumeOk::read(r)?),
            Basic::CANCEL => Self::BasicCancel(Cancel::read(r)?),
            Basic::CANCEL_OK => Self::BasicCancelOk(CancelOk::read(r)?),
            Basic::RETURN => Self::BasicReturn(Return::read(r, payload)?),
            Basic::DELIVER => Self::BasicDeliver(Deliver::read(r, payload)?),
            Basic::GET_OK => Self::BasicGetOk(GetOk::read(r, payload)?),
            Basic::GET_EMPTY => Self::BasicGetEmpty(GetEmpty::read(r)?),
            Basic::ACK => Self::BasicAck(Ack::read(r)?),
            Basic::NACK => Self::BasicNack(Nack::read(r)?),
            Basic::RECOVER_OK => Self::BasicRecoverOk(RecoverOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::BASIC,
                    method_id: id,
                });
            }
        })
    }

    fn parse_confirm(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match method_id {
            Confirm::SELECT_OK => Self::ConfirmSelectOk(SelectOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::CONFIRM,
                    method_id: id,
                });
            }
        })
    }

    fn parse_tx(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        use tx::SelectOk;

        Ok(match method_id {
            Tx::SELECT_OK => Self::TxSelectOk(SelectOk::read(r)?),
            Tx::COMMIT_OK => Self::TxCommitOk(CommitOk::read(r)?),
            Tx::ROLLBACK_OK => Self::TxRollbackOk(RollbackOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::TX,
                    method_id: id,
                });
            }
        })
    }

    pub fn is_content_method(&self) -> bool {
        matches!(
            self,
            Self::BasicDeliver(_) | Self::BasicReturn(_) | Self::BasicGetOk(_)
        )
    }
}
