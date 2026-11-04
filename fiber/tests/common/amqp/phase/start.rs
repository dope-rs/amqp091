use crate::common::amqp::io::Read;

use crate::common::amqp::Error;
use crate::common::amqp::phase::Started;
use crate::common::amqp::wire::incoming::Incoming;
use crate::common::amqp::wire::route::Route;

/// Connection.Start sent and awaiting Connection.StartOk.
#[repr(transparent)]
pub struct Start<'a, IO> {
    pub(crate) io: &'a mut IO,
}

impl<'a, IO: Read> Start<'a, IO> {
    pub fn recv(self) -> Result<Started<'a, IO>, Error> {
        let incoming = Incoming::read(&mut *self.io)?;
        let expected = Route::new(0, 10, 11);
        if incoming.route() != expected {
            return Err(Error::route(expected, incoming.route()));
        }
        incoming.discard()?;
        Ok(Started { io: self.io })
    }
}
