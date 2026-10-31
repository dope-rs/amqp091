use std::io::Write;

use crate::common::amqp::phase::Start;
use crate::common::amqp::wire::Method;
use crate::common::amqp::{Error, Greeting};

/// Client preface accepted and ready for the server greeting.
#[repr(transparent)]
pub struct Preface<'a, IO> {
    pub(crate) io: &'a mut IO,
}

impl<'a, IO: Write> Preface<'a, IO> {
    pub fn start(self, greeting: Greeting<'_>) -> Result<Start<'a, IO>, Error> {
        let (major, minor, auth, langs) = greeting.parts();
        let auth_len = u32::try_from(auth.len())
            .map_err(Error::Length)?
            .to_be_bytes();
        let lang_len = u32::try_from(langs.len())
            .map_err(Error::Length)?
            .to_be_bytes();
        let prefix = [major, minor, 0, 0, 0, 0];
        Method::write_parts(
            self.io,
            0,
            10,
            10,
            &[&prefix, &auth_len, auth, &lang_len, langs],
        )
        .map_err(Error::Io)?;
        Ok(Start { io: self.io })
    }

    /// Sends a phase-specific method and releases the connection borrow.
    pub(crate) fn method(
        self,
        channel: u16,
        class: u16,
        method: u16,
        args: &[u8],
    ) -> Result<(), Error> {
        Method::write(self.io, channel, class, method, args).map_err(Error::Io)?;
        Ok(())
    }
}
