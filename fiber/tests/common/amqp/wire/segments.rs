use std::io::{IoSlice, Write};
use std::{array, io};

pub(super) struct Segments<'a, const N: usize> {
    parts: [&'a [u8]; N],
    first: usize,
    offset: usize,
}

impl<'a, const N: usize> Segments<'a, N> {
    pub(super) const fn new(parts: [&'a [u8]; N]) -> Self {
        Self {
            parts,
            first: 0,
            offset: 0,
        }
    }

    pub(super) fn write(mut self, sink: &mut impl Write) -> io::Result<()> {
        while self.first < N {
            let parts = &self.parts[self.first..];
            let mut views: [IoSlice<'_>; N] =
                array::from_fn(|index| IoSlice::new(&self.parts[index][..0]));
            for (index, (view, bytes)) in views.iter_mut().zip(parts).enumerate() {
                let offset = usize::from(index == 0) * self.offset;
                *view = IoSlice::new(&bytes[offset..]);
            }
            let written = sink.write_vectored(&views[..parts.len()])?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write AMQP frame",
                ));
            }
            self.advance(written);
        }
        Ok(())
    }

    fn advance(&mut self, mut written: usize) {
        while self.first < N {
            let left = self.parts[self.first].len() - self.offset;
            if written < left {
                self.offset += written;
                return;
            }
            written -= left;
            self.first += 1;
            self.offset = 0;
            if written == 0 {
                return;
            }
        }
    }
}
