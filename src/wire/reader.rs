use std::ops::Range;
use std::str::from_utf8;

use crate::error::Error;

pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.buf.len() < n {
            return Err(Error::Protocol("short read"));
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    pub fn take_u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    pub fn take_i8(&mut self) -> Result<i8, Error> {
        Ok(self.take(1)?[0] as i8)
    }

    fn take_array<const N: usize>(&mut self) -> Result<&'a [u8; N], Error> {
        self.take(N)?
            .try_into()
            .map_err(|_| Error::Protocol("short read"))
    }

    pub fn take_u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(*self.take_array()?))
    }

    pub fn take_i16(&mut self) -> Result<i16, Error> {
        Ok(i16::from_be_bytes(*self.take_array()?))
    }

    pub fn take_u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(*self.take_array()?))
    }

    pub fn take_i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(*self.take_array()?))
    }

    pub fn take_u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(*self.take_array()?))
    }

    pub fn take_i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(*self.take_array()?))
    }

    pub fn take_f32(&mut self) -> Result<f32, Error> {
        Ok(f32::from_be_bytes(*self.take_array()?))
    }

    pub fn take_f64(&mut self) -> Result<f64, Error> {
        Ok(f64::from_be_bytes(*self.take_array()?))
    }

    pub fn take_short_str(&mut self) -> Result<&'a str, Error> {
        let n = self.take_u8()? as usize;
        let bytes = self.take(n)?;
        from_utf8(bytes).map_err(|_| Error::Protocol("invalid utf-8 in short-str"))
    }

    pub(crate) fn take_short_str_range(&mut self, base: &[u8]) -> Result<Range<usize>, Error> {
        let n = self.take_u8()? as usize;
        let start = (self.buf.as_ptr() as usize)
            .checked_sub(base.as_ptr() as usize)
            .ok_or(Error::Protocol("reader base mismatch"))?;
        let bytes = self.take(n)?;
        from_utf8(bytes).map_err(|_| Error::Protocol("invalid utf-8 in short-str"))?;
        Ok(start..start + n)
    }

    pub fn take_long_str(&mut self) -> Result<&'a [u8], Error> {
        let n = self.take_u32()? as usize;
        self.take(n)
    }
}
