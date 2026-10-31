use crate::failure;
use crate::protocol::class;
use crate::protocol::method::conn;

#[repr(u8)]
#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Start,
    Secure,
    Tune,
    Open,
    Close,
    CloseOk,
    Blocked,
    Unblocked,
}

impl Kind {
    pub(crate) fn identify(id: u16) -> Result<Self, failure::Error> {
        Ok(match id {
            value if value == conn::START.method() => Self::Start,
            value if value == conn::SECURE.method() => Self::Secure,
            value if value == conn::TUNE.method() => Self::Tune,
            value if value == conn::OPEN_OK.method() => Self::Open,
            value if value == conn::CLOSE.method() => Self::Close,
            value if value == conn::CLOSE_OK.method() => Self::CloseOk,
            value if value == conn::BLOCKED.method() => Self::Blocked,
            value if value == conn::UNBLOCKED.method() => Self::Unblocked,
            _ => {
                return Err(failure::Error::UnknownMethod {
                    class_id: class::CONNECTION.get(),
                    method_id: id,
                });
            }
        })
    }
}

const _: () = assert!(size_of::<Kind>() == 1);
