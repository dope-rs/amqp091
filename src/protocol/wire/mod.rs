use o3::buffer::pool;
use o3::buffer::write::{Sink as _, consume};

use crate::protocol::wire::text::{long, short};

pub(crate) mod bytes;
pub mod content;
mod encoded;
pub mod frame;
pub(crate) mod outputs;
pub mod reader;
pub(crate) mod schema;
pub mod text;

pub(crate) use encoded::Encoded;

/// Contiguous, transactional output used by protocol encoders.
/// Implementations retain the first exact failure until the frame is finished.
/// Generic encoders remain statically dispatched across pooled cursors.
pub trait Write {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn status(&self) -> Result<(), consume::Error>;

    fn push(&mut self, byte: u8);

    fn extend(&mut self, bytes: &[u8]);
}

/// An AMQP frame under construction; only successful completion yields a payload.
pub struct Writer {
    lease: pool::Cursor,
    status: Result<(), consume::Error>,
}

impl Writer {
    pub const fn new(lease: pool::Cursor) -> Self {
        Self {
            lease,
            status: Ok(()),
        }
    }

    pub fn try_finish(self) -> Result<pool::Cursor, consume::Error> {
        self.status?;
        Ok(self.lease)
    }

    pub(in crate::protocol::wire) fn position(&self) -> u32 {
        self.lease.len() as u32
    }
}

impl Write for Writer {
    fn len(&self) -> usize {
        self.lease.len()
    }

    fn status(&self) -> Result<(), consume::Error> {
        self.status
    }

    fn push(&mut self, byte: u8) {
        if self.status.is_err() {
            return;
        }
        self.status = self.lease.write_byte(byte);
    }

    fn extend(&mut self, bytes: &[u8]) {
        if self.status.is_err() {
            return;
        }
        self.status = self.lease.write_slice(bytes);
    }
}

impl AsMut<[u8]> for Writer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.lease.as_mut()
    }
}

const _: () =
    assert!(size_of::<Writer>() == size_of::<pool::Cursor>() + size_of::<consume::Error>());

#[derive(Clone, Copy)]
pub struct Fingerprint(u64);

const FINGERPRINT_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FINGERPRINT_PRIME: u64 = 0x100_0000_01b3;

impl Fingerprint {
    pub(crate) const fn new() -> Self {
        Self(FINGERPRINT_OFFSET)
    }

    pub(crate) fn of(bytes: &[u8]) -> Self {
        let mut hash = Self::new();
        hash.write(bytes);
        hash
    }

    pub(crate) fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FINGERPRINT_PRIME);
        }
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Identity<'a> {
    bytes: &'a [u8],
    fingerprint: Fingerprint,
}

impl<'a> Identity<'a> {
    pub(crate) fn of(bytes: &'a [u8]) -> Self {
        Self::new(bytes, Fingerprint::of(bytes))
    }

    pub(crate) const fn new(bytes: &'a [u8], hash: Fingerprint) -> Self {
        Self {
            bytes,
            fingerprint: hash,
        }
    }

    pub(crate) const fn bytes(self) -> &'a [u8] {
        self.bytes
    }

    pub(crate) const fn fingerprint(self) -> Fingerprint {
        self.fingerprint
    }
}

/// AMQP primitive writer.
#[doc(hidden)]
pub trait Put: Write {
    fn put_u8(&mut self, value: u8) {
        self.push(value);
    }

    fn put_i8(&mut self, value: i8) {
        self.push(value as u8);
    }

    fn put_u16(&mut self, value: u16) {
        self.extend(&value.to_be_bytes());
    }

    fn put_i16(&mut self, value: i16) {
        self.extend(&value.to_be_bytes());
    }

    fn put_u32(&mut self, value: u32) {
        self.extend(&value.to_be_bytes());
    }

    fn put_i32(&mut self, value: i32) {
        self.extend(&value.to_be_bytes());
    }

    fn put_u64(&mut self, value: u64) {
        self.extend(&value.to_be_bytes());
    }

    fn put_i64(&mut self, value: i64) {
        self.extend(&value.to_be_bytes());
    }

    fn put_f32(&mut self, value: f32) {
        self.extend(&value.to_be_bytes());
    }

    fn put_f64(&mut self, value: f64) {
        self.extend(&value.to_be_bytes());
    }

    fn put_bool(&mut self, value: bool) {
        self.push(if value { 1 } else { 0 });
    }

    fn put_bits<const N: usize>(&mut self, flags: [bool; N]) {
        let mut byte = 0u8;
        let mut count = 0u8;
        for flag in flags {
            if flag {
                byte |= 1 << count;
            }
            count += 1;
            if count == 8 {
                self.push(byte);
                byte = 0;
                count = 0;
            }
        }
        if count != 0 {
            self.push(byte);
        }
    }

    fn put_short(&mut self, value: short::Short<'_>) {
        self.push(value.len());
        self.extend(value.value().as_bytes());
    }

    fn put_name(&mut self, value: short::Name<'_>) {
        self.push(value.len());
        self.extend(value.value().as_bytes());
    }

    fn put_long(&mut self, value: long::Long<'_>) {
        self.put_u32(value.len());
        self.extend(value.value());
    }
}
