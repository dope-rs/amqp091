use std::io::{IoSlice, Read, Write};
use std::{cmp, io};

pub struct Duplex<const IN: usize, const OUT: usize> {
    pub(crate) input: [u8; IN],
    pub(crate) output: [u8; OUT],
    pub(crate) read: usize,
    pub(crate) written: usize,
    pub(crate) read_max: usize,
    pub(crate) write_max: usize,
    pub(crate) writes: usize,
    pub(crate) vectors: [usize; 3],
}

impl<const IN: usize, const OUT: usize> Read for Duplex<IN, OUT> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let input = self
            .input
            .get(self.read..)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fixture read offset"))?;
        let length = cmp::min(buffer.len(), cmp::min(self.read_max, input.len()));
        buffer[..length].copy_from_slice(&input[..length]);
        self.read += length;
        Ok(length)
    }
}

impl<const IN: usize, const OUT: usize> Write for Duplex<IN, OUT> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let output = self
            .output
            .get_mut(self.written..)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fixture write offset"))?;
        let length = cmp::min(buffer.len(), cmp::min(self.write_max, output.len()));
        output[..length].copy_from_slice(&buffer[..length]);
        self.written += length;
        Ok(length)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        let output = self
            .output
            .get_mut(self.written..)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fixture write offset"))?;
        let writes = self
            .writes
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fixture write count"))?;
        if let Some(length) = self.vectors.get_mut(self.writes) {
            *length = buffers.len();
        }
        self.writes = writes;
        let mut capacity = cmp::min(self.write_max, output.len());
        let mut written = 0;
        for buffer in buffers {
            let length = cmp::min(buffer.len(), capacity);
            output[written..written + length].copy_from_slice(&buffer[..length]);
            written += length;
            capacity -= length;
            if capacity == 0 {
                break;
            }
        }
        self.written += written;
        Ok(written)
    }
}
