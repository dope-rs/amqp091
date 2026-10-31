use std::{mem, slice, str};

use o3::buffer;
use o3::depot::array::copy;

use crate::failure;
use crate::failure::wire::field::Fault;
use crate::failure::wire::{self, Encoding};
use crate::message::field::view::tag::Tag;
use crate::message::{field, property};
use crate::protocol::wire::text::short;
use crate::protocol::wire::{Put, reader};

mod raw;
mod tag;

const MAX_DEPTH: usize = 64;
const MAX_NODES: usize = 65_536;

/// A suffix of an immutable field body admitted through TableSpan. Only table
/// and array iteration construct cursors; their entry points and exhaustive
/// decoding preserve the validated key, tag, payload, and nested boundaries.
struct Checked<'a>(&'a [u8]);

impl<'a> Checked<'a> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn array<const N: usize>(&mut self) -> [u8; N] {
        let source = self.0;
        // SAFETY: the exhaustive tag decoder follows the validated payload shape.
        let value = unsafe { *source.as_ptr().cast::<[u8; N]>() };
        // SAFETY: validation proved that this encoded value contains N bytes.
        self.0 = unsafe { source.get_unchecked(N..) };
        value
    }

    fn byte(&mut self) -> u8 {
        self.array::<1>()[0]
    }

    fn bytes(&mut self, len: usize) -> &'a [u8] {
        let source = self.0;
        // SAFETY: validation proved the exact length of every encoded field value.
        let value = unsafe { source.get_unchecked(..len) };
        // SAFETY: the same proof establishes that len is within source.
        self.0 = unsafe { source.get_unchecked(len..) };
        value
    }

    fn short(&mut self) -> &'a str {
        let len = self.byte() as usize;
        let bytes = self.bytes(len);
        // SAFETY: table validation checked every short field name as UTF-8.
        unsafe { str::from_utf8_unchecked(bytes) }
    }

    fn long(&mut self) -> &'a [u8] {
        let len = u32::from_be_bytes(self.array()) as usize;
        self.bytes(len)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct TableSpan {
    start: u32,
    end: u32,
}

impl TableSpan {
    pub(in crate::message) fn try_new(start: usize, body: &[u8]) -> Result<Self, failure::Error> {
        let len = mem::size_of::<u32>() + body.len();
        let max = u32::MAX as usize;
        let Some(end) = start.checked_add(len).filter(|end| *end <= max) else {
            return Err(failure::Error::Field(Fault::Span { start, len, max }));
        };
        validate_table(body, start + mem::size_of::<u32>())?;
        Ok(Self {
            start: start as u32,
            end: end as u32,
        })
    }
}

#[derive(Clone, Copy)]
pub struct Table<'a> {
    source: Source<&'a field::Table, &'a [u8]>,
}

#[derive(Clone, Copy)]
enum Source<O, W> {
    Owned(O),
    Wire(W),
}

impl<S: buffer::raw::Source> property::Properties<S, TableSpan> {
    pub fn headers(&self) -> Option<Table<'_>> {
        let span = self.headers.as_ref()?;
        let start = span.start as usize + mem::size_of::<u32>();
        let end = span.end as usize;
        // SAFETY: Fields::try_table established this span with a successful read
        // from this owner's source and validated every encoded field. Exact
        // compaction and try_clone retain bytes and offsets. Properties exposes
        // no mutable source/span projection, and Source keeps the backing, range
        // and contents for this shared borrow.
        Some(Table::wire(unsafe {
            self.source().as_ref().get_unchecked(start..end)
        }))
    }
}

impl<'a> Table<'a> {
    pub(crate) const fn owned(table: &'a field::Table) -> Self {
        Self {
            source: Source::Owned(table),
        }
    }

    const fn wire(body: &'a [u8]) -> Self {
        Self {
            source: Source::Wire(body),
        }
    }

    pub fn iter(self) -> TableIter<'a> {
        let source = match self.source {
            Source::Owned(table) => Source::Owned(table.entries()),
            Source::Wire(body) => Source::Wire(Checked(body)),
        };
        TableIter { source }
    }

    pub fn is_empty(self) -> bool {
        match self.source {
            Source::Owned(table) => table.entries().len() == 0,
            Source::Wire(body) => body.is_empty(),
        }
    }

    pub(crate) fn encoded_len(self) -> u32 {
        match self.source {
            Source::Owned(table) => table.encoded_len(),
            Source::Wire(body) => body.len() as u32,
        }
    }

    pub(crate) fn encode<S: Put>(self, output: &mut S) {
        match self.source {
            Source::Owned(table) => table.encode(output),
            Source::Wire(body) => {
                output.put_u32(body.len() as u32);
                output.extend(body);
            }
        }
    }
}

pub struct TableIter<'a> {
    source: Source<slice::Iter<'a, (short::Buf, field::Value)>, Checked<'a>>,
}

impl<'a> Iterator for TableIter<'a> {
    type Item = (&'a str, Value<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.source {
            Source::Owned(entries) => entries
                .next()
                .map(|(key, value)| (key.as_ref(), value.view())),
            Source::Wire(input) => (!input.is_empty()).then(|| decode_entry(input)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.source {
            Source::Owned(entries) => entries.size_hint(),
            Source::Wire(_) => (0, None),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Array<'a> {
    source: Source<&'a [field::Value], &'a [u8]>,
}

impl<'a> Array<'a> {
    pub(super) const fn owned(values: &'a [field::Value]) -> Self {
        Self {
            source: Source::Owned(values),
        }
    }

    const fn wire(body: &'a [u8]) -> Self {
        Self {
            source: Source::Wire(body),
        }
    }

    pub fn iter(self) -> ArrayIter<'a> {
        let source = match self.source {
            Source::Owned(values) => Source::Owned(values.iter()),
            Source::Wire(body) => Source::Wire(Checked(body)),
        };
        ArrayIter { source }
    }

    pub fn is_empty(self) -> bool {
        match self.source {
            Source::Owned(values) => values.is_empty(),
            Source::Wire(body) => body.is_empty(),
        }
    }
}

pub struct ArrayIter<'a> {
    source: Source<slice::Iter<'a, field::Value>, Checked<'a>>,
}

impl<'a> Iterator for ArrayIter<'a> {
    type Item = Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.source {
            Source::Owned(values) => values.next().map(field::Value::view),
            Source::Wire(input) => (!input.is_empty()).then(|| decode_value(input)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.source {
            Source::Owned(values) => values.size_hint(),
            Source::Wire(_) => (0, None),
        }
    }
}

#[derive(Clone, Copy)]
pub enum Value<'a> {
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    Decimal { scale: u8, value: i32 },
    String(&'a [u8]),
    Array(Array<'a>),
    Timestamp(u64),
    Table(Table<'a>),
    Void,
    Bytes(&'a [u8]),
}

fn decode_entry<'a>(input: &mut Checked<'a>) -> (&'a str, Value<'a>) {
    (input.short(), decode_value(input))
}

fn decode_value<'a>(input: &mut Checked<'a>) -> Value<'a> {
    // SAFETY: the two private iterator entry points admit a nonempty value;
    // table iteration first consumes its validated key. Every arm consumes the
    // exact admitted payload, retaining the next entry or value boundary.
    match unsafe { raw::tag(&mut input.0) } {
        Tag::Bool => Value::Bool(input.byte() != 0),
        Tag::I8 => Value::I8(input.byte() as i8),
        Tag::U8 => Value::U8(input.byte()),
        Tag::I16 | Tag::I16Alias => Value::I16(i16::from_be_bytes(input.array())),
        Tag::U16 => Value::U16(u16::from_be_bytes(input.array())),
        Tag::I32 => Value::I32(i32::from_be_bytes(input.array())),
        Tag::U32 => Value::U32(u32::from_be_bytes(input.array())),
        Tag::U64 => Value::U64(u64::from_be_bytes(input.array())),
        Tag::I64 => Value::I64(i64::from_be_bytes(input.array())),
        Tag::F32 => Value::F32(f32::from_be_bytes(input.array())),
        Tag::F64 => Value::F64(f64::from_be_bytes(input.array())),
        Tag::Decimal => Value::Decimal {
            scale: input.byte(),
            value: i32::from_be_bytes(input.array()),
        },
        Tag::String => Value::String(input.long()),
        Tag::Array => Value::Array(Array::wire(input.long())),
        Tag::Timestamp => Value::Timestamp(u64::from_be_bytes(input.array())),
        Tag::Table => Value::Table(Table::wire(input.long())),
        Tag::Void => Value::Void,
        Tag::Bytes => Value::Bytes(input.long()),
    }
}

#[derive(Clone, Copy)]
enum Container {
    Table,
    Array,
}

#[derive(Clone, Copy)]
struct Frame<'a> {
    kind: Container,
    reader: reader::Reader<'a>,
    end: u32,
}

impl<'a> Frame<'a> {
    fn offset(&self) -> usize {
        self.end as usize - self.reader.len()
    }

    fn try_read<const N: usize>(&mut self) -> Result<&'a [u8; N], failure::Error> {
        let offset = self.offset();
        let (bytes, reader) = self
            .reader
            .take_array()
            .map_err(|error| failure::Error::Field(Fault::Read(reader::Fault { offset, error })))?;
        self.reader = reader;
        Ok(bytes)
    }

    fn try_short(&mut self) -> Result<&'a str, failure::Error> {
        let offset = self.offset();
        let value = self
            .reader
            .short()
            .map_err(|error| failure::Error::Field(Fault::Read(reader::Fault { offset, error })))?;
        if let Some(at) = value.bytes().position(|byte| byte == 0) {
            return Err(failure::Error::Protocol(wire::Fault::TextNul {
                kind: Encoding::Short,
                offset: offset + 1 + at,
            }));
        }
        Ok(value)
    }

    fn try_long(&mut self) -> Result<&'a [u8], failure::Error> {
        let offset = self.offset();
        self.reader
            .long()
            .map_err(|error| failure::Error::Field(Fault::Read(reader::Fault { offset, error })))
    }

    fn try_next(&mut self) -> Result<Option<Nested<'a>>, failure::Error> {
        if matches!(self.kind, Container::Table) {
            self.try_short()?;
        }
        try_value(self)
    }
}

struct Nested<'a> {
    kind: Container,
    body: &'a [u8],
    end: u32,
}

fn try_value<'a>(frame: &mut Frame<'a>) -> Result<Option<Nested<'a>>, failure::Error> {
    let offset = frame.offset();
    let &[tag] = frame.try_read::<1>()?;
    let nested = match tag {
        tag::BOOL | tag::I8 | tag::U8 => {
            frame.try_read::<1>()?;
            None
        }
        tag::I16 | tag::I16_ALIAS | tag::U16 => {
            frame.try_read::<2>()?;
            None
        }
        tag::I32 | tag::U32 | tag::F32 => {
            frame.try_read::<4>()?;
            None
        }
        tag::U64 | tag::I64 | tag::F64 | tag::TIMESTAMP => {
            frame.try_read::<8>()?;
            None
        }
        tag::DECIMAL => {
            frame.try_read::<1>()?;
            frame.try_read::<4>()?;
            None
        }
        tag::STRING | tag::BYTES => {
            frame.try_long()?;
            None
        }
        tag::ARRAY | tag::TABLE => {
            let body = frame.try_long()?;
            let end = frame.offset() as u32;
            let kind = if tag == tag::ARRAY {
                Container::Array
            } else {
                Container::Table
            };
            Some(Nested { kind, body, end })
        }
        tag::VOID => None,
        actual => {
            return Err(failure::Error::Field(Fault::Tag { offset, actual }));
        }
    };
    Ok(nested)
}

fn validate_table(body: &[u8], start: usize) -> Result<(), failure::Error> {
    let end = (start + body.len()) as u32;
    let mut frames = copy::Vec::<_, MAX_DEPTH>::from_one(Frame {
        kind: Container::Table,
        reader: reader::Reader::new(body),
        end,
    });
    let mut nodes = 0usize;

    while let Some(frame) = frames.last_mut() {
        if frame.reader.is_empty() {
            let _ = frames.pop();
            continue;
        }
        let nested = frame.try_next()?;

        if nodes == MAX_NODES {
            return Err(failure::Error::Field(Fault::Nodes {
                actual: MAX_NODES + 1,
                max: MAX_NODES,
            }));
        }
        nodes += 1;

        let Some(nested) = nested else {
            continue;
        };
        let Some(vacant) = frames.vacancy() else {
            return Err(failure::Error::Field(Fault::Depth {
                actual: MAX_DEPTH + 1,
                max: MAX_DEPTH,
            }));
        };
        vacant.insert(Frame {
            kind: nested.kind,
            reader: reader::Reader::new(nested.body),
            end: nested.end,
        });
    }
    Ok(())
}

const _: () = {
    assert!(mem::size_of::<Checked<'_>>() == mem::size_of::<&[u8]>());
    assert!(mem::size_of::<Frame<'_>>() == 3 * mem::size_of::<usize>());
};
