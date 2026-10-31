use std::io;
use std::io::Read;

use crate::common::amqp::wire::header::Header;

pub(super) struct Input<'a, IO> {
    source: &'a mut IO,
    left: u32,
}

impl<'a, IO: Read> Input<'a, IO> {
    pub(super) fn read(source: &'a mut IO) -> io::Result<(Header, Self)> {
        let header = Header::read(source)?;
        let left = header.length();
        Ok((header, Self { source, left }))
    }

    pub(super) fn read_exact(&mut self, buffer: &mut [u8]) -> io::Result<()> {
        let length = u32::try_from(buffer.len())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        if length > self.left {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "AMQP frame payload is truncated",
            ));
        }
        self.source.read_exact(buffer)?;
        self.left -= length;
        Ok(())
    }

    pub(super) fn discard(mut self) -> io::Result<()> {
        let mut buffer = [0; 256];
        while self.left != 0 {
            let length = self.left.min(buffer.len() as u32) as usize;
            self.read_exact(&mut buffer[..length])?;
        }
        self.finish()
    }

    pub(super) fn compare(mut self, expected: &[&[u8]]) -> io::Result<()> {
        let mut length = 0_usize;
        for part in expected {
            length = length.checked_add(part.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "AMQP comparison is too large")
            })?;
        }
        if self.left as usize != length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected AMQP method arguments length",
            ));
        }
        let mut buffer = [0; 256];
        for part in expected {
            for chunk in part.chunks(buffer.len()) {
                self.read_exact(&mut buffer[..chunk.len()])?;
                if &buffer[..chunk.len()] != chunk {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected AMQP method arguments",
                    ));
                }
            }
        }
        self.finish()
    }

    fn finish(self) -> io::Result<()> {
        if self.left != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AMQP frame payload was not consumed",
            ));
        }
        let mut end = [0];
        self.source.read_exact(&mut end)?;
        if end[0] != super::END {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid AMQP frame end",
            ));
        }
        Ok(())
    }
}
