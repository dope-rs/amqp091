use o3::collections::SlabKey;

use crate::error::Error;
use crate::method::{basic, queue};
use crate::protocol::PendingResponse;

pub(in crate::runtime) struct RegisteredConsumer {
    pub(in crate::runtime) method: basic::ConsumeOk,
    pub(in crate::runtime) key: SlabKey,
}

pub trait Extract: Sized {
    fn extract(r: PendingResponse) -> Result<Self, Error>;
}

impl Extract for () {
    fn extract(r: PendingResponse) -> Result<Self, Error> {
        match r {
            PendingResponse::Empty => Ok(()),
            _ => Err(Error::Protocol("unexpected response variant")),
        }
    }
}

impl Extract for queue::DeclareOk {
    fn extract(r: PendingResponse) -> Result<Self, Error> {
        match r {
            PendingResponse::QueueDeclareOk(ok) => Ok(ok),
            _ => Err(Error::Protocol("unexpected response variant")),
        }
    }
}

impl Extract for queue::PurgeOk {
    fn extract(r: PendingResponse) -> Result<Self, Error> {
        match r {
            PendingResponse::QueuePurgeOk(ok) => Ok(ok),
            _ => Err(Error::Protocol("unexpected response variant")),
        }
    }
}

impl Extract for queue::DeleteOk {
    fn extract(r: PendingResponse) -> Result<Self, Error> {
        match r {
            PendingResponse::QueueDeleteOk(ok) => Ok(ok),
            _ => Err(Error::Protocol("unexpected response variant")),
        }
    }
}

impl Extract for basic::ConsumeOk {
    fn extract(r: PendingResponse) -> Result<Self, Error> {
        match r {
            PendingResponse::BasicConsumeOk(ok, _) => Ok(ok),
            _ => Err(Error::Protocol("unexpected response variant")),
        }
    }
}

impl Extract for RegisteredConsumer {
    fn extract(r: PendingResponse) -> Result<Self, Error> {
        match r {
            PendingResponse::BasicConsumeOk(method, key) => Ok(Self { method, key }),
            _ => Err(Error::Protocol("unexpected response variant")),
        }
    }
}

impl Extract for basic::CancelOk {
    fn extract(r: PendingResponse) -> Result<Self, Error> {
        match r {
            PendingResponse::BasicCancelOk(ok) => Ok(ok),
            _ => Err(Error::Protocol("unexpected response variant")),
        }
    }
}
