use std::ops;

use crate::failure::{self, terminal};
use crate::protocol::wire::text::resident;

/// Runtime operation failure without materializing a transport cause.

#[derive(Debug)]
pub enum Runtime<'c, 'b, 'd, const ID: u8> {
    Local(failure::Error<resident::Short<'b, 'd>>),
    Terminal(failure::Cause<'c, 'b, 'd, ID>),
}

impl<'c, 'b, 'd, const ID: u8> Runtime<'c, 'b, 'd, ID> {
    pub(crate) fn terminal(cause: terminal::Lease<'c, 'b, 'd>) -> Self {
        Self::Terminal(failure::Cause::from_raw(cause))
    }

    pub fn error(&self) -> &failure::Error<resident::Short<'b, 'd>> {
        match self {
            Self::Local(error) => error,
            Self::Terminal(cause) => cause.error(),
        }
    }
}

impl<'c, 'b, 'd, const ID: u8> From<failure::Error> for Runtime<'c, 'b, 'd, ID> {
    fn from(error: failure::Error) -> Self {
        Self::Local(error.into())
    }
}

impl<'c, 'b, 'd, const ID: u8> ops::Deref for Runtime<'c, 'b, 'd, ID> {
    type Target = failure::Error<resident::Short<'b, 'd>>;

    fn deref(&self) -> &Self::Target {
        self.error()
    }
}
