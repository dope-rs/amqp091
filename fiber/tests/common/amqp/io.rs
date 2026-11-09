use dope_test::harness::peer::Stream;

use super::Error;

pub trait Read {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error>;

    fn read_exact(&mut self, mut buffer: &mut [u8]) -> Result<(), Error> {
        while !buffer.is_empty() {
            let count = self.read(buffer)?;
            if count == 0 {
                return Err(Error::UnexpectedEof);
            }
            buffer = &mut buffer[count..];
        }
        Ok(())
    }
}

pub trait Write {
    fn write_vectored(&mut self, buffers: &[&[u8]]) -> Result<usize, Error>;
}

impl Read for Stream<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
        Stream::read(self, buffer).map_err(Error::Io)
    }
}

impl Read for &Stream<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
        Stream::read(self, buffer).map_err(Error::Io)
    }
}

impl Write for Stream<'_> {
    fn write_vectored(&mut self, buffers: &[&[u8]]) -> Result<usize, Error> {
        Write::write_vectored(&mut &*self, buffers)
    }
}

impl Write for &Stream<'_> {
    fn write_vectored(&mut self, buffers: &[&[u8]]) -> Result<usize, Error> {
        Stream::write_vectored(self, buffers).map_err(Error::Io)
    }
}

impl Read for &[u8] {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
        let count = buffer.len().min(self.len());
        buffer[..count].copy_from_slice(&self[..count]);
        *self = &self[count..];
        Ok(count)
    }
}
