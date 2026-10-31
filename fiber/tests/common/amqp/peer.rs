use std::io::{Read, Write};

use crate::common::amqp::phase::Preface;
use crate::common::amqp::{Error, Greeting, Tune};

/// Borrowed broker peer at the beginning of an AMQP handshake.
#[repr(transparent)]
pub struct Peer<'a, IO> {
    io: &'a mut IO,
}

impl<'a, IO> Peer<'a, IO> {
    pub const fn new(io: &'a mut IO) -> Self {
        Self { io }
    }
}

impl<'a, IO: Read> Peer<'a, IO> {
    pub fn preface(self) -> Result<Preface<'a, IO>, Error> {
        let mut actual = [0; 8];
        self.io.read_exact(&mut actual).map_err(Error::Io)?;
        if actual != *b"AMQP\0\0\x09\x01" {
            return Err(Error::Preface(actual));
        }
        Ok(Preface { io: self.io })
    }
}

impl<IO: Read + Write> Peer<'_, IO> {
    /// Completes the canonical PLAIN/en_US server handshake.
    pub fn open(self, tune: Tune) -> Result<(), Error> {
        let started = self.preface()?.start(Greeting::PLAIN)?.recv()?;
        let open = started.tune(tune)?.recv()?;
        open.recv()?.send()
    }
}
