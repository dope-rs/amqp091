use crate::Error;
use crate::method::{basic, queue};
use crate::protocol::PendingResponse;

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
            PendingResponse::BasicConsumeOk(ok) => Ok(ok),
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
