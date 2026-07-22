use super::put::Put;
use super::reader::Reader;
use super::sink::Sink;
use crate::error::Error;

#[derive(Clone, Debug)]
pub enum FieldValue {
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
    Decimal { scale: u8, value: u32 },
    ShortStr(String),
    LongStr(Vec<u8>),
    Array(Vec<FieldValue>),
    Timestamp(u64),
    Table(FieldTable),
    Void,
    ByteArray(Vec<u8>),
}

#[derive(Clone, Debug, Default)]
pub struct FieldTable {
    entries: Vec<(String, FieldValue)>,
}

impl FieldTable {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn insert(&mut self, key: impl Into<String>, value: FieldValue) -> &mut Self {
        self.entries.push((key.into(), value));
        self
    }

    pub fn encode<S: Sink>(&self, out: &mut S) {
        let pos = out.reserve_u32();
        let start = out.len();
        for (k, v) in &self.entries {
            out.put_short_str(k);
            v.encode(out);
        }
        let len = (out.len() - start) as u32;
        out.finish_u32_at(pos, len);
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        let n = r.take_u32()? as usize;
        let body = r.take(n)?;
        let mut inner = Reader::new(body);
        let mut entries = Vec::new();
        while !inner.is_empty() {
            let k = inner.take_short_str()?.to_string();
            let v = FieldValue::decode(&mut inner)?;
            entries.push((k, v));
        }
        Ok(Self { entries })
    }
}

impl FieldValue {
    pub fn encode<S: Sink>(&self, out: &mut S) {
        match self {
            Self::Bool(v) => {
                out.put_u8(b't');
                out.put_bool(*v);
            }
            Self::I8(v) => {
                out.put_u8(b'b');
                out.put_i8(*v);
            }
            Self::U8(v) => {
                out.put_u8(b'B');
                out.put_u8(*v);
            }
            Self::I16(v) => {
                out.put_u8(b's');
                out.put_i16(*v);
            }
            Self::U16(v) => {
                out.put_u8(b'u');
                out.put_u16(*v);
            }
            Self::I32(v) => {
                out.put_u8(b'I');
                out.put_i32(*v);
            }
            Self::U32(v) => {
                out.put_u8(b'i');
                out.put_u32(*v);
            }
            Self::I64(v) => {
                out.put_u8(b'l');
                out.put_i64(*v);
            }
            Self::U64(v) => {
                out.put_u8(b'L');
                out.put_u64(*v);
            }
            Self::F32(v) => {
                out.put_u8(b'f');
                out.put_f32(*v);
            }
            Self::F64(v) => {
                out.put_u8(b'd');
                out.put_f64(*v);
            }
            Self::Decimal { scale, value } => {
                out.put_u8(b'D');
                out.put_u8(*scale);
                out.put_u32(*value);
            }
            Self::ShortStr(s) => {
                out.put_u8(b'S');
                out.put_long_str(s.as_bytes());
            }
            Self::LongStr(s) => {
                out.put_u8(b'S');
                out.put_long_str(s);
            }
            Self::Array(items) => {
                out.put_u8(b'A');
                let pos = out.reserve_u32();
                let start = out.len();
                for item in items {
                    item.encode(out);
                }
                let len = (out.len() - start) as u32;
                out.finish_u32_at(pos, len);
            }
            Self::Timestamp(v) => {
                out.put_u8(b'T');
                out.put_u64(*v);
            }
            Self::Table(t) => {
                out.put_u8(b'F');
                t.encode(out);
            }
            Self::Void => {
                out.put_u8(b'V');
            }
            Self::ByteArray(b) => {
                out.put_u8(b'x');
                out.put_long_str(b);
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        let tag = r.take_u8()?;
        Ok(match tag {
            b't' => Self::Bool(r.take_u8()? != 0),
            b'b' => Self::I8(r.take_i8()?),
            b'B' => Self::U8(r.take_u8()?),
            b'U' | b's' => Self::I16(r.take_i16()?),
            b'u' => Self::U16(r.take_u16()?),
            b'I' => Self::I32(r.take_i32()?),
            b'i' => Self::U32(r.take_u32()?),
            b'L' => Self::U64(r.take_u64()?),
            b'l' => Self::I64(r.take_i64()?),
            b'f' => Self::F32(r.take_f32()?),
            b'd' => Self::F64(r.take_f64()?),
            b'D' => {
                let scale = r.take_u8()?;
                let value = r.take_u32()?;
                Self::Decimal { scale, value }
            }
            b'S' => {
                let bytes = r.take_long_str()?;
                Self::LongStr(bytes.to_vec())
            }
            b'A' => {
                let n = r.take_u32()? as usize;
                let body = r.take(n)?;
                let mut inner = Reader::new(body);
                let mut items = Vec::new();
                while !inner.is_empty() {
                    items.push(FieldValue::decode(&mut inner)?);
                }
                Self::Array(items)
            }
            b'T' => Self::Timestamp(r.take_u64()?),
            b'F' => Self::Table(FieldTable::decode(r)?),
            b'V' => Self::Void,
            b'x' => {
                let bytes = r.take_long_str()?;
                Self::ByteArray(bytes.to_vec())
            }
            other => {
                return Err(Error::ProtocolOwned(format!(
                    "unknown field-value tag 0x{:02x}",
                    other
                )));
            }
        })
    }
}
