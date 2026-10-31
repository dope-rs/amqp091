use std::io::Write;

use crate::common::amqp::Error;
use crate::common::amqp::wire::Method;

/// Connection.Open received and ready for Connection.OpenOk.
#[repr(transparent)]
pub struct Opened<'a, IO> {
    pub(crate) io: &'a mut IO,
}

impl<IO: Write> Opened<'_, IO> {
    pub fn send(self) -> Result<(), Error> {
        Method::write(self.io, 0, 10, 41, &[0]).map_err(Error::Io)?;
        Ok(())
    }
}
