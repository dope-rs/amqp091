use std::io::Read;

use crate::common::amqp::Error;
use crate::common::amqp::phase::Opened;
use crate::common::amqp::wire::incoming::Incoming;
use crate::common::amqp::wire::route::Route;

/// Connection.TuneOk received and awaiting Connection.Open.
#[repr(transparent)]
pub struct Open<'a, IO> {
    pub(crate) io: &'a mut IO,
}

impl<'a, IO: Read> Open<'a, IO> {
    pub fn recv(self) -> Result<Opened<'a, IO>, Error> {
        let incoming = Incoming::read(&mut *self.io).map_err(Error::Io)?;
        let expected = Route::new(0, 10, 40);
        if incoming.route() != expected {
            return Err(Error::route(expected, incoming.route()));
        }
        incoming.discard().map_err(Error::Io)?;
        Ok(Opened { io: self.io })
    }
}
