use std::io::Write;

use crate::common::amqp::phase::{Secure, Tuning};
use crate::common::amqp::wire::Method;
use crate::common::amqp::{Error, Tune};

/// Connection.StartOk received and ready for challenge or tuning.
#[repr(transparent)]
pub struct Started<'a, IO> {
    pub(crate) io: &'a mut IO,
}

impl<'a, IO: Write> Started<'a, IO> {
    pub fn secure(self, nonce: &[u8]) -> Result<Secure<'a, IO>, Error> {
        let length = u32::try_from(nonce.len())
            .map_err(Error::Length)?
            .to_be_bytes();
        Method::write_parts(self.io, 0, 10, 20, &[&length, nonce]).map_err(Error::Io)?;
        Ok(Secure { io: self.io })
    }

    pub fn tune(self, tune: Tune) -> Result<Tuning<'a, IO>, Error> {
        let (channels, frame, interval) = tune.parts();
        let channels = channels.to_be_bytes();
        let frame = frame.to_be_bytes();
        let interval = interval.to_be_bytes();
        let args = [
            channels[0],
            channels[1],
            frame[0],
            frame[1],
            frame[2],
            frame[3],
            interval[0],
            interval[1],
        ];
        Method::write(self.io, 0, 10, 30, &args).map_err(Error::Io)?;
        Ok(Tuning { io: self.io })
    }
}
