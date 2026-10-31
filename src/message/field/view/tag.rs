use std::mem;

#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Tag {
    Bool = b't',
    I8 = b'b',
    U8 = b'B',
    I16 = b's',
    I16Alias = b'U',
    U16 = b'u',
    I32 = b'I',
    U32 = b'i',
    I64 = b'l',
    U64 = b'L',
    F32 = b'f',
    F64 = b'd',
    Decimal = b'D',
    String = b'S',
    Array = b'A',
    Timestamp = b'T',
    Table = b'F',
    Void = b'V',
    Bytes = b'x',
}

pub(super) const BOOL: u8 = Tag::Bool as u8;
pub(super) const I8: u8 = Tag::I8 as u8;
pub(super) const U8: u8 = Tag::U8 as u8;
pub(super) const I16: u8 = Tag::I16 as u8;
pub(super) const I16_ALIAS: u8 = Tag::I16Alias as u8;
pub(super) const U16: u8 = Tag::U16 as u8;
pub(super) const I32: u8 = Tag::I32 as u8;
pub(super) const U32: u8 = Tag::U32 as u8;
pub(super) const I64: u8 = Tag::I64 as u8;
pub(super) const U64: u8 = Tag::U64 as u8;
pub(super) const F32: u8 = Tag::F32 as u8;
pub(super) const F64: u8 = Tag::F64 as u8;
pub(super) const DECIMAL: u8 = Tag::Decimal as u8;
pub(super) const STRING: u8 = Tag::String as u8;
pub(super) const ARRAY: u8 = Tag::Array as u8;
pub(super) const TIMESTAMP: u8 = Tag::Timestamp as u8;
pub(super) const TABLE: u8 = Tag::Table as u8;
pub(super) const VOID: u8 = Tag::Void as u8;
pub(super) const BYTES: u8 = Tag::Bytes as u8;

const _: () = {
    assert!(mem::size_of::<Tag>() == 1);
    assert!(mem::align_of::<Tag>() == 1);
};
