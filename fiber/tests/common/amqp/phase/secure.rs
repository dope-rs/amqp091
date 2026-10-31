use std::io::Read;

use crate::common::amqp::Error;
use crate::common::amqp::phase::Started;
use crate::common::amqp::wire::incoming::Incoming;
use crate::common::amqp::wire::route::Route;

/// Connection.Secure sent and awaiting Connection.SecureOk.
#[repr(transparent)]
pub struct Secure<'a, IO> {
    pub(crate) io: &'a mut IO,
}

impl<'a, IO: Read> Secure<'a, IO> {
    pub fn recv(self, response: &[u8]) -> Result<Started<'a, IO>, Error> {
        let length = u32::try_from(response.len())
            .map_err(Error::Length)?
            .to_be_bytes();
        let incoming = Incoming::read(&mut *self.io).map_err(Error::Io)?;
        let expected = Route::new(0, 10, 21);
        if incoming.route() != expected {
            return Err(Error::route(expected, incoming.route()));
        }
        incoming.compare(&[&length, response]).map_err(Error::Io)?;
        Ok(Started { io: self.io })
    }
}
