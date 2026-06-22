pub mod basic;
pub mod channel;
pub mod confirm;
pub mod connection;
mod encode;
pub mod exchange;
pub mod queue;
mod spec;
pub mod tx;

pub use encode::ClientMethod;
pub use spec::{Basic, Channel, ClassId, Confirm, Connection, Exchange, Method, Queue, Tx};

use crate::Error;
use crate::wire::Reader;

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
    pub fn parse(payload: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::new(payload);
        let class_id = r.take_u16()?;
        let method_id = r.take_u16()?;
        match class_id {
            ClassId::CONNECTION => Self::parse_connection(method_id, &mut r),
            ClassId::CHANNEL => Self::parse_channel(method_id, &mut r),
            ClassId::EXCHANGE => Self::parse_exchange(method_id, &mut r),
            ClassId::QUEUE => Self::parse_queue(method_id, &mut r),
            ClassId::BASIC => Self::parse_basic(method_id, &mut r),
            ClassId::CONFIRM => Self::parse_confirm(method_id, &mut r),
            ClassId::TX => Self::parse_tx(method_id, &mut r),
            other => Err(Error::UnknownClass(other)),
        }
    }

    fn parse_connection(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match method_id {
            Connection::START => Self::ConnectionStart(connection::Start::read(r)?),
            Connection::SECURE => Self::ConnectionSecure(connection::Secure::read(r)?),
            Connection::TUNE => Self::ConnectionTune(connection::Tune::read(r)?),
            Connection::OPEN_OK => Self::ConnectionOpenOk(connection::OpenOk::read(r)?),
            Connection::CLOSE => Self::ConnectionClose(connection::Close::read(r)?),
            Connection::CLOSE_OK => Self::ConnectionCloseOk(connection::CloseOk::read(r)?),
            Connection::BLOCKED => Self::ConnectionBlocked(connection::Blocked::read(r)?),
            Connection::UNBLOCKED => Self::ConnectionUnblocked(connection::Unblocked::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::CONNECTION,
                    method_id: id,
                });
            }
        })
    }

    fn parse_channel(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match method_id {
            Channel::OPEN_OK => Self::ChannelOpenOk(channel::OpenOk::read(r)?),
            Channel::FLOW => Self::ChannelFlow(channel::Flow::read(r)?),
            Channel::FLOW_OK => Self::ChannelFlowOk(channel::FlowOk::read(r)?),
            Channel::CLOSE => Self::ChannelClose(channel::Close::read(r)?),
            Channel::CLOSE_OK => Self::ChannelCloseOk(channel::CloseOk::read(r)?),
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
            Exchange::DECLARE_OK => Self::ExchangeDeclareOk(exchange::DeclareOk::read(r)?),
            Exchange::DELETE_OK => Self::ExchangeDeleteOk(exchange::DeleteOk::read(r)?),
            Exchange::BIND_OK => Self::ExchangeBindOk(exchange::BindOk::read(r)?),
            Exchange::UNBIND_OK => Self::ExchangeUnbindOk(exchange::UnbindOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::EXCHANGE,
                    method_id: id,
                });
            }
        })
    }

    fn parse_queue(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match method_id {
            Queue::DECLARE_OK => Self::QueueDeclareOk(queue::DeclareOk::read(r)?),
            Queue::BIND_OK => Self::QueueBindOk(queue::BindOk::read(r)?),
            Queue::UNBIND_OK => Self::QueueUnbindOk(queue::UnbindOk::read(r)?),
            Queue::PURGE_OK => Self::QueuePurgeOk(queue::PurgeOk::read(r)?),
            Queue::DELETE_OK => Self::QueueDeleteOk(queue::DeleteOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::QUEUE,
                    method_id: id,
                });
            }
        })
    }

    fn parse_basic(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match method_id {
            Basic::QOS_OK => Self::BasicQosOk(basic::QosOk::read(r)?),
            Basic::CONSUME_OK => Self::BasicConsumeOk(basic::ConsumeOk::read(r)?),
            Basic::CANCEL => Self::BasicCancel(basic::Cancel::read(r)?),
            Basic::CANCEL_OK => Self::BasicCancelOk(basic::CancelOk::read(r)?),
            Basic::RETURN => Self::BasicReturn(basic::Return::read(r)?),
            Basic::DELIVER => Self::BasicDeliver(basic::Deliver::read(r)?),
            Basic::GET_OK => Self::BasicGetOk(basic::GetOk::read(r)?),
            Basic::GET_EMPTY => Self::BasicGetEmpty(basic::GetEmpty::read(r)?),
            Basic::ACK => Self::BasicAck(basic::Ack::read(r)?),
            Basic::NACK => Self::BasicNack(basic::Nack::read(r)?),
            Basic::RECOVER_OK => Self::BasicRecoverOk(basic::RecoverOk::read(r)?),
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
            Confirm::SELECT_OK => Self::ConfirmSelectOk(confirm::SelectOk::read(r)?),
            id => {
                return Err(Error::UnknownMethod {
                    class_id: ClassId::CONFIRM,
                    method_id: id,
                });
            }
        })
    }

    fn parse_tx(method_id: u16, r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(match method_id {
            Tx::SELECT_OK => Self::TxSelectOk(tx::SelectOk::read(r)?),
            Tx::COMMIT_OK => Self::TxCommitOk(tx::CommitOk::read(r)?),
            Tx::ROLLBACK_OK => Self::TxRollbackOk(tx::RollbackOk::read(r)?),
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
