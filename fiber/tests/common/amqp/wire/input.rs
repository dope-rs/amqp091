use crate::common::amqp::Error;
use crate::common::amqp::io::Read;

use crate::common::amqp::wire::header::Header;

pub(super) struct Input<'a, IO> {
    source: &'a mut IO,
    left: u32,
}

impl<'a, IO: Read> Input<'a, IO> {
    pub(super) fn read(source: &'a mut IO) -> Result<(Header, Self), Error> {
        let header = Header::read(source)?;
        let left = header.length();
        Ok((header, Self { source, left }))
    }

    pub(super) fn read_exact(&mut self, buffer: &mut [u8]) -> Result<(), Error> {
        let length = u32::try_from(buffer.len()).map_err(Error::Length)?;
        if length > self.left {
            return Err(Error::Truncated {
                offset: length as usize,
                length: self.left as usize,
            });
        }
        self.source.read_exact(buffer)?;
        self.left -= length;
        Ok(())
    }

    pub(super) fn discard(mut self) -> Result<(), Error> {
        let mut buffer = [0; 256];
        while self.left != 0 {
            let length = self.left.min(buffer.len() as u32) as usize;
            self.read_exact(&mut buffer[..length])?;
        }
        self.finish()
    }

    pub(super) fn compare(mut self, expected: &[&[u8]]) -> Result<(), Error> {
        let mut length = 0_usize;
        for part in expected {
            length = length
                .checked_add(part.len())
                .ok_or_else(|| Error::Capacity { length: part.len() })?;
        }
        if self.left as usize != length {
            return Err(Error::PayloadLength {
                expected: length,
                actual: self.left as usize,
            });
        }
        let mut buffer = [0; 256];
        for part in expected {
            for chunk in part.chunks(buffer.len()) {
                self.read_exact(&mut buffer[..chunk.len()])?;
                if &buffer[..chunk.len()] != chunk {
                    return Err(Error::Payload {
                        expected: chunk.to_vec(),
                        actual: buffer[..chunk.len()].to_vec(),
                    });
                }
            }
        }
        self.finish()
    }

    fn finish(self) -> Result<(), Error> {
        if self.left != 0 {
            return Err(Error::PayloadRemaining(self.left));
        }
        let mut end = [0];
        self.source.read_exact(&mut end)?;
        if end[0] != super::END {
            return Err(Error::FrameEnd(end[0]));
        }
        Ok(())
    }
}
