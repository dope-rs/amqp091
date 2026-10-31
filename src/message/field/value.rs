use crate::failure;
use crate::failure::wire::Encoding;
use crate::message::field::{self, view};
use crate::protocol::wire::Put;
use crate::protocol::wire::text::long;

pub struct Value {
    kind: Kind,
    len: u32,
}

pub(super) enum Kind {
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
    String(long::Buf),
    Array(Vec<Value>),
    Timestamp(u64),
    Table(field::Table),
    Void,
    Bytes(long::Buf),
}

impl Value {
    fn fixed(kind: Kind, body: u32) -> Self {
        Self {
            kind,
            len: 1 + body,
        }
    }

    pub fn decimal(scale: u8, value: i32) -> Self {
        Self::fixed(Kind::Decimal { scale, value }, 5)
    }

    pub fn string(value: Vec<u8>) -> Result<Self, failure::Error> {
        let value = long::Buf::new(value)?;
        let len = field::sum(Encoding::Long, value.as_long().len(), 5)?;
        Ok(Self {
            kind: Kind::String(value),
            len,
        })
    }

    pub fn bytes(value: Vec<u8>) -> Result<Self, failure::Error> {
        let value = long::Buf::new(value)?;
        let len = field::sum(Encoding::Long, value.as_long().len(), 5)?;
        Ok(Self {
            kind: Kind::Bytes(value),
            len,
        })
    }

    pub fn array(items: Vec<Self>) -> Result<Self, failure::Error> {
        let body = items.iter().try_fold(0u32, |left, item| {
            field::sum(Encoding::FieldArray, left, item.len)
        })?;
        let len = field::sum(Encoding::FieldArray, body, 5)?;
        Ok(Self {
            kind: Kind::Array(items),
            len,
        })
    }

    pub fn table(value: field::Table) -> Result<Self, failure::Error> {
        let len = field::sum(Encoding::FieldTable, value.encoded_len(), 5)?;
        Ok(Self {
            kind: Kind::Table(value),
            len,
        })
    }

    pub fn timestamp(value: u64) -> Self {
        Self::fixed(Kind::Timestamp(value), 8)
    }

    pub fn void() -> Self {
        Self::fixed(Kind::Void, 0)
    }

    pub(crate) fn encoded_len(&self) -> u32 {
        self.len
    }

    pub(crate) fn encode<S: Put>(&self, output: &mut S) {
        match &self.kind {
            Kind::Bool(value) => {
                output.put_u8(b't');
                output.put_bool(*value);
            }
            Kind::I8(value) => {
                output.put_u8(b'b');
                output.put_i8(*value);
            }
            Kind::U8(value) => {
                output.put_u8(b'B');
                output.put_u8(*value);
            }
            Kind::I16(value) => {
                output.put_u8(b's');
                output.put_i16(*value);
            }
            Kind::U16(value) => {
                output.put_u8(b'u');
                output.put_u16(*value);
            }
            Kind::I32(value) => {
                output.put_u8(b'I');
                output.put_i32(*value);
            }
            Kind::U32(value) => {
                output.put_u8(b'i');
                output.put_u32(*value);
            }
            Kind::I64(value) => {
                output.put_u8(b'l');
                output.put_i64(*value);
            }
            Kind::U64(value) => {
                output.put_u8(b'L');
                output.put_u64(*value);
            }
            Kind::F32(value) => {
                output.put_u8(b'f');
                output.put_f32(*value);
            }
            Kind::F64(value) => {
                output.put_u8(b'd');
                output.put_f64(*value);
            }
            Kind::Decimal { scale, value } => {
                output.put_u8(b'D');
                output.put_u8(*scale);
                output.put_i32(*value);
            }
            Kind::String(value) => {
                output.put_u8(b'S');
                output.put_long(value.as_long());
            }
            Kind::Array(items) => {
                output.put_u8(b'A');
                output.put_u32(self.len - 5);
                for item in items {
                    item.encode(output);
                }
            }
            Kind::Timestamp(value) => {
                output.put_u8(b'T');
                output.put_u64(*value);
            }
            Kind::Table(table) => {
                output.put_u8(b'F');
                table.encode(output);
            }
            Kind::Void => output.put_u8(b'V'),
            Kind::Bytes(value) => {
                output.put_u8(b'x');
                output.put_long(value.as_long());
            }
        }
    }

    pub fn view(&self) -> view::Value<'_> {
        match &self.kind {
            Kind::Bool(value) => view::Value::Bool(*value),
            Kind::I8(value) => view::Value::I8(*value),
            Kind::U8(value) => view::Value::U8(*value),
            Kind::I16(value) => view::Value::I16(*value),
            Kind::U16(value) => view::Value::U16(*value),
            Kind::I32(value) => view::Value::I32(*value),
            Kind::U32(value) => view::Value::U32(*value),
            Kind::I64(value) => view::Value::I64(*value),
            Kind::U64(value) => view::Value::U64(*value),
            Kind::F32(value) => view::Value::F32(*value),
            Kind::F64(value) => view::Value::F64(*value),
            Kind::Decimal { scale, value } => view::Value::Decimal {
                scale: *scale,
                value: *value,
            },
            Kind::String(value) => view::Value::String(value.as_long().value()),
            Kind::Array(values) => view::Value::Array(view::Array::owned(values)),
            Kind::Timestamp(value) => view::Value::Timestamp(*value),
            Kind::Table(table) => view::Value::Table(view::Table::owned(table)),
            Kind::Void => view::Value::Void,
            Kind::Bytes(value) => view::Value::Bytes(value.as_long().value()),
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::fixed(Kind::Bool(value), 1)
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Self::fixed(Kind::I8(value), 1)
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Self::fixed(Kind::U8(value), 1)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Self::fixed(Kind::I16(value), 2)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Self::fixed(Kind::U16(value), 2)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::fixed(Kind::I32(value), 4)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::fixed(Kind::U32(value), 4)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::fixed(Kind::I64(value), 8)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::fixed(Kind::U64(value), 8)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::fixed(Kind::F32(value), 4)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::fixed(Kind::F64(value), 8)
    }
}
