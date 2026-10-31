use std::ops;

use crate::failure::{self, terminal};

pub(crate) enum State<'c, 'b, 'd, const ID: u8> {
    Active(terminal::Lease<'c, 'b, 'd>),
    Retired(failure::Exit<'c, 'b, 'd, ID>),
}

impl<'c, 'b, 'd, const ID: u8> State<'c, 'b, 'd, ID> {
    pub(crate) fn retire(self, reason: failure::Reason) -> Self {
        let mut exit = match self {
            Self::Active(cause) => failure::Exit { cause, reason },
            Self::Retired(exit) => exit,
        };
        exit.reason = reason;
        Self::Retired(exit)
    }

    pub(crate) fn exit(&self) -> Option<&failure::Exit<'c, 'b, 'd, ID>> {
        match self {
            Self::Active(_) => None,
            Self::Retired(exit) => Some(exit),
        }
    }
}

impl<'c, 'b, 'd, const ID: u8> ops::Deref for State<'c, 'b, 'd, ID> {
    type Target = terminal::Lease<'c, 'b, 'd>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Active(cause) => cause,
            Self::Retired(exit) => &exit.cause,
        }
    }
}
