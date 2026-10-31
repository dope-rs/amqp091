use std::{mem, str};

pub trait Primitive: Sized {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Read {
        offset: usize,
        requested: usize,
        remaining: usize,
    },
    Utf8 {
        offset: usize,
        error: str::Utf8Error,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fault {
    pub offset: usize,
    pub error: Error,
}

impl Error {
    const fn at(self, offset: usize) -> Self {
        match self {
            Self::Read {
                requested: count,
                remaining: left,
                ..
            } => Self::Read {
                offset,
                requested: count,
                remaining: left,
            },
            Self::Utf8 { error, .. } => Self::Utf8 { offset, error },
        }
    }
}

#[derive(Clone, Copy)]
pub struct Reader<'a> {
    source: &'a [u8],
}

const _: () = assert!(mem::size_of::<Reader<'_>>() == mem::size_of::<&[u8]>());

impl<'a> Reader<'a> {
    pub const fn new(source: &'a [u8]) -> Self {
        Self { source }
    }

    pub const fn is_empty(&self) -> bool {
        self.source.is_empty()
    }

    pub(crate) const fn len(&self) -> usize {
        self.source.len()
    }

    fn take(self, count: usize) -> Result<(&'a [u8], Self), Error> {
        let left = self.source.len();
        let Some((bytes, source)) = self.source.split_at_checked(count) else {
            return Err(Error::Read {
                offset: 0,
                requested: count,
                remaining: left,
            });
        };
        Ok((bytes, Self { source }))
    }

    pub(crate) fn take_array<const N: usize>(self) -> Result<(&'a [u8; N], Self), Error> {
        let left = self.source.len();
        let Some((bytes, source)) = self.source.split_first_chunk::<N>() else {
            return Err(Error::Read {
                offset: 0,
                requested: N,
                remaining: left,
            });
        };
        Ok((bytes, Self { source }))
    }

    pub fn bytes(&mut self, count: usize) -> Result<&'a [u8], Error> {
        let (bytes, reader) = self.take(count)?;
        *self = reader;
        Ok(bytes)
    }

    pub fn read<T: Primitive>(&mut self) -> Result<T, Error> {
        let (value, reader) = T::read(*self)?;
        *self = reader;
        Ok(value)
    }

    fn take_short(self) -> Result<(&'a str, Self), Error> {
        let (count, reader) = u8::read(self)?;
        let (bytes, reader) = reader
            .take(count as usize)
            .map_err(|error| error.at(mem::size_of::<u8>()))?;
        match str::from_utf8(bytes) {
            Ok(value) => Ok((value, reader)),
            Err(error) => Err(Error::Utf8 {
                offset: mem::size_of::<u8>(),
                error,
            }),
        }
    }

    pub fn short(&mut self) -> Result<&'a str, Error> {
        let (value, reader) = self.take_short()?;
        *self = reader;
        Ok(value)
    }

    fn take_long(self) -> Result<(&'a [u8], Self), Error> {
        let (count, reader) = u32::read(self)?;
        let (bytes, reader) = reader
            .take(count as usize)
            .map_err(|error| error.at(mem::size_of::<u32>()))?;
        Ok((bytes, reader))
    }

    pub fn long(&mut self) -> Result<&'a [u8], Error> {
        let (bytes, reader) = self.take_long()?;
        *self = reader;
        Ok(bytes)
    }

    pub fn long_str(&mut self) -> Result<&'a str, Error> {
        let (bytes, reader) = self.take_long()?;
        let value = str::from_utf8(bytes).map_err(|error| Error::Utf8 {
            offset: mem::size_of::<u32>(),
            error,
        })?;
        *self = reader;
        Ok(value)
    }
}

impl Primitive for u8 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<1>()?;
        Ok((bytes[0], reader))
    }
}

impl Primitive for i8 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<1>()?;
        Ok((bytes[0] as Self, reader))
    }
}

impl Primitive for u16 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<2>()?;
        Ok((Self::from_be_bytes(*bytes), reader))
    }
}

impl Primitive for i16 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<2>()?;
        Ok((Self::from_be_bytes(*bytes), reader))
    }
}

impl Primitive for u32 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<4>()?;
        Ok((Self::from_be_bytes(*bytes), reader))
    }
}

impl Primitive for i32 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<4>()?;
        Ok((Self::from_be_bytes(*bytes), reader))
    }
}

impl Primitive for u64 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<8>()?;
        Ok((Self::from_be_bytes(*bytes), reader))
    }
}

impl Primitive for i64 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<8>()?;
        Ok((Self::from_be_bytes(*bytes), reader))
    }
}

impl Primitive for f32 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<4>()?;
        Ok((Self::from_be_bytes(*bytes), reader))
    }
}

impl Primitive for f64 {
    fn read<'a>(reader: Reader<'a>) -> Result<(Self, Reader<'a>), Error> {
        let (bytes, reader) = reader.take_array::<8>()?;
        Ok((Self::from_be_bytes(*bytes), reader))
    }
}
