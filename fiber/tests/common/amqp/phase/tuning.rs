use std::io::Read;

use crate::common::amqp::Error;
use crate::common::amqp::phase::Open;
use crate::common::amqp::wire::incoming::Incoming;
use crate::common::amqp::wire::route::Route;

/// Connection.Tune sent and awaiting Connection.TuneOk.
#[repr(transparent)]
pub struct Tuning<'a, IO> {
    pub(crate) io: &'a mut IO,
}

impl<'a, IO: Read> Tuning<'a, IO> {
    pub fn recv(self) -> Result<Open<'a, IO>, Error> {
        let incoming = Incoming::read(&mut *self.io).map_err(Error::Io)?;
        let expected = Route::new(0, 10, 31);
        if incoming.route() != expected {
            return Err(Error::route(expected, incoming.route()));
        }
        incoming.discard().map_err(Error::Io)?;
        Ok(Open { io: self.io })
    }
}
