use crate::failure::{self, terminal};
use crate::protocol::wire::text::resident;

/// One retired connection's original failure and observed transport reason.
pub struct Exit<'c, 'b, 'd, const ID: u8> {
    pub(super) cause: terminal::Lease<'c, 'b, 'd>,
    pub(super) reason: failure::Reason,
}

impl<'c, 'b, 'd, const ID: u8> Exit<'c, 'b, 'd, ID> {
    pub fn generation(&self) -> u64 {
        self.cause.generation()
    }

    pub const fn reason(&self) -> failure::Reason {
        self.reason
    }

    pub fn error(&self) -> &failure::Error<resident::Short<'b, 'd>> {
        self.cause.error()
    }
}
