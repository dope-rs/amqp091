use std::{marker, mem, str};

use o3::buffer;
use o3::buffer::hold::scope;
use wire::field;

use crate::failure::{self, wire};
use crate::protocol::wire::text::resident::Exact;
use crate::protocol::wire::text::short;
use crate::protocol::wire::{reader, schema};

pub(crate) struct Decoder<'a, Schema, P = [Span; 0]> {
    reader: reader::Reader<'a>,
    len: usize,
    fields: P,
    schema: marker::PhantomData<fn(Schema) -> Schema>,
}

pub(crate) enum Decode {
    Failure(failure::Error),
    Read(reader::Fault),
    Utf8 {
        offset: usize,
        error: str::Utf8Error,
    },
    Nul {
        offset: usize,
    },
    Trailing {
        offset: usize,
        remaining: usize,
    },
}

pub(crate) struct Args<'view, 'input, 'd, R>
where
    R: buffer::Cursor + 'input,
{
    source: &'view scope::Deferred<'input, 'd, R>,
}

impl From<failure::Error> for Decode {
    fn from(error: failure::Error) -> Self {
        Self::Failure(error)
    }
}

impl From<reader::Error> for Decode {
    fn from(error: reader::Error) -> Self {
        Self::Read(reader::Fault { offset: 0, error })
    }
}

impl Decode {
    fn into_error(self) -> failure::Error {
        match self {
            Self::Failure(error) => error,
            Self::Read(error) => failure::Error::Decode(error),
            Self::Utf8 { offset, error } => failure::Error::Decode(reader::Fault {
                offset,
                error: reader::Error::Utf8 { offset: 0, error },
            }),
            Self::Nul { offset } => failure::Error::Protocol(wire::Fault::TextNul {
                kind: wire::Encoding::Short,
                offset,
            }),
            Self::Trailing {
                offset,
                remaining: left,
            } => failure::Error::Field(field::Fault::Trailing {
                offset,
                remaining: left,
            }),
        }
    }

    fn at(self, consumed: usize) -> Self {
        match self {
            Self::Read(reader::Fault { error, .. }) => Self::Read(reader::Fault {
                offset: consumed,
                error,
            }),
            error => error,
        }
    }
}

impl<'view, 'input, 'd, R> Args<'view, 'input, 'd, R>
where
    R: buffer::Cursor + 'input,
{
    pub(crate) const fn new(source: &'view scope::Deferred<'input, 'd, R>) -> Self {
        Self { source }
    }

    pub(crate) fn decode<T, E>(
        self,
        decode: impl FnOnce(&mut reader::Reader<'input>) -> Result<T, E>,
    ) -> Result<T, failure::Error>
    where
        E: Into<Decode>,
    {
        let len = self.source.len();
        let mut input = reader::Reader::new(self.source.as_slice());
        let value =
            decode(&mut input).map_err(|error| error.into().at(len - input.len()).into_error())?;
        if !input.is_empty() {
            return Err(Decode::Trailing {
                offset: len - input.len(),
                remaining: input.len(),
            }
            .into_error());
        }
        Ok(value)
    }

    pub(crate) fn empty(self) -> Result<(), failure::Error> {
        if self.source.is_empty() {
            Ok(())
        } else {
            Err(Decode::Trailing {
                offset: 0,
                remaining: self.source.len(),
            }
            .into_error())
        }
    }
}

impl<'a, Schema> Decoder<'a, Schema> {
    fn new(source: &'a [u8]) -> Self {
        Self {
            reader: reader::Reader::new(source),
            len: source.len(),
            fields: [],
            schema: marker::PhantomData,
        }
    }
}

impl<'a, Schema, P> Decoder<'a, Schema, P> {
    pub(crate) fn read<T: reader::Primitive>(&mut self) -> Result<T, Decode> {
        let consumed = self.len - self.reader.len();
        self.reader.read().map_err(|error| {
            Decode::Read(reader::Fault {
                offset: consumed,
                error,
            })
        })
    }
}

impl<Schema, const N: usize> Decoder<'_, Schema, [Span; N]> {
    fn finish(self) -> Result<[Span; N], Decode> {
        if !self.reader.is_empty() {
            let left = self.reader.len();
            Err(Decode::Trailing {
                offset: self.len - left,
                remaining: left,
            })
        } else {
            Ok(self.fields)
        }
    }
}

impl<'a, Schema, P: schema::Append<Span>> Decoder<'a, Schema, P> {
    pub(crate) fn short<const INDEX: usize>(
        self,
        _field: schema::Field<Schema, INDEX>,
    ) -> Result<(short::Short<'a>, Decoder<'a, Schema, P::Next>), Decode> {
        const { assert!(INDEX == P::LEN) };
        let (reader, span, valid) = Span::read(self.reader, self.len)?;
        Ok((
            valid,
            Decoder {
                reader,
                len: self.len,
                fields: self.fields.append(span),
                schema: self.schema,
            },
        ))
    }
}

fn decode<'a, Schema, const N: usize, T>(
    source: &'a [u8],
    parse: impl FnOnce(Decoder<'a, Schema>) -> Result<(T, Decoder<'a, Schema, [Span; N]>), Decode>,
) -> Result<(T, [Span; N]), Decode> {
    let (value, decoder) = parse(Decoder::new(source))?;
    Ok((value, decoder.finish()?))
}

pub(crate) struct View<'a, Schema, const N: usize> {
    bytes: &'a [u8],
    fields: [Span; N],
    schema: marker::PhantomData<fn(Schema) -> Schema>,
}

#[derive(Clone, Copy)]
pub(crate) struct Packed<Schema> {
    pub(super) ends: [u16; 3],
    schema: marker::PhantomData<fn(Schema) -> Schema>,
}

pub(crate) const PACKED_CAPACITY: usize = 3 * u8::MAX as usize;

impl<Schema> Decoder<'_, Schema> {
    pub(crate) fn decode<'input, 'd, R: buffer::Cursor + 'input>(
        source: &scope::Deferred<'input, 'd, R>,
        field: schema::Field<Schema, 0>,
    ) -> Result<short::Short<'input>, failure::Error> {
        let (valid, _) = decode::<Schema, 1, _>(source.as_slice(), |decoder| decoder.short(field))
            .map_err(Decode::into_error)?;
        Ok(valid)
    }

    pub(crate) fn hold<'input, 'd, R: buffer::Cursor + 'input, T>(
        source: &scope::Deferred<'input, 'd, R>,
        field: schema::Field<Schema, 0>,
        parse: impl FnOnce(&mut Decoder<'input, Schema, [Span; 1]>) -> Result<T, Decode>,
    ) -> Result<(T, short::Short<'input>), failure::Error> {
        let ((value, valid), _) = decode(source.as_slice(), |decoder| {
            let (valid, mut decoder) = decoder.short(field)?;
            Ok(((parse(&mut decoder)?, valid), decoder))
        })
        .map_err(Decode::into_error)?;
        Ok((value, valid))
    }

    pub(crate) fn retain<'input, 'd, R: buffer::Cursor + 'input, T>(
        source: &scope::Deferred<'input, 'd, R>,
        budget: &buffer::resident::Budget<'d>,
        parse: impl FnOnce(
            Decoder<'input, Schema>,
        ) -> Result<
            (
                (T, short::Short<'input>),
                Decoder<'input, Schema, [Span; 1]>,
            ),
            Decode,
        >,
    ) -> Result<(T, Exact<'d>), failure::Error> {
        let ((value, valid), _) = decode(source.as_slice(), parse).map_err(Decode::into_error)?;
        Ok((value, valid.try_copy(wire::Allocation::Short, budget)?))
    }
}

impl<'input, Schema, const N: usize> View<'input, Schema, N> {
    pub(crate) fn decode<'d, R: buffer::Cursor + 'input, T>(
        source: &scope::Deferred<'input, 'd, R>,
        parse: impl FnOnce(
            Decoder<'input, Schema>,
        ) -> Result<(T, Decoder<'input, Schema, [Span; N]>), Decode>,
    ) -> Result<(T, Self), failure::Error> {
        const { assert!(N > 0) };
        let bytes = source.as_slice();
        let (value, fields) = decode(bytes, parse).map_err(Decode::into_error)?;
        Ok((
            value,
            Self {
                bytes,
                fields,
                schema: marker::PhantomData,
            },
        ))
    }

    pub(crate) fn get<const INDEX: usize>(
        &self,
        field: schema::Field<Schema, INDEX>,
    ) -> &'input str {
        const { assert!(INDEX < N) };
        self.fields[field.index()].view(self.bytes)
    }
}

impl<Schema> Packed<Schema> {
    pub(crate) fn copy_from(
        source: &View<'_, Schema, 3>,
        target: &mut [mem::MaybeUninit<u8>; PACKED_CAPACITY],
    ) -> Self {
        let mut ends = [0; 3];
        let mut start = 0usize;
        for (from, to) in source.fields.iter().zip(&mut ends) {
            let value = from.view(source.bytes).as_bytes();
            let end = start + usize::from(from.len());
            target[start..end].write_copy_of_slice(value);
            *to = end as u16;
            start = end;
        }
        Self {
            ends,
            schema: marker::PhantomData,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Span {
    start: u32,
    len: u8,
}

impl Span {
    const fn new(start: u32, len: u8) -> Self {
        Self { start, len }
    }

    pub(super) const fn start(self) -> usize {
        self.start as usize
    }

    pub(super) const fn end(self) -> usize {
        self.start as usize + self.len as usize
    }

    const fn len(self) -> u8 {
        self.len
    }

    fn read<'a>(
        mut reader: reader::Reader<'a>,
        len: usize,
    ) -> Result<(reader::Reader<'a>, Self, short::Short<'a>), Decode> {
        let consumed = len - reader.len();
        let count = reader.read::<u8>().map_err(|error| {
            Decode::Read(reader::Fault {
                offset: consumed,
                error,
            })
        })?;
        let start = len - reader.len();
        let bytes = reader.bytes(usize::from(count)).map_err(|error| {
            Decode::Read(reader::Fault {
                offset: start,
                error,
            })
        })?;
        let text = str::from_utf8(bytes).map_err(|error| Decode::Utf8 {
            offset: start,
            error,
        })?;
        if let Some(offset) = text.find('\0') {
            return Err(Decode::Nul {
                offset: start + offset,
            });
        }
        Ok((reader, Self::new(start as u32, count), short::Short(text)))
    }
}
