use dope::manifold::connector::session::Stage;
use o3::buffer::Owned;

pub trait Sink {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, src: &[u8]);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn as_mut_slice(&mut self) -> &mut [u8];
}

impl Sink for Owned {
    fn push(&mut self, byte: u8) {
        Owned::push(self, byte);
    }
    fn extend_from_slice(&mut self, src: &[u8]) {
        Owned::extend_from_slice(self, src);
    }
    fn len(&self) -> usize {
        Owned::len(self)
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        Owned::as_mut_slice(self)
    }
}

impl Sink for Stage<'_> {
    fn push(&mut self, byte: u8) {
        Stage::push(self, byte);
    }
    fn extend_from_slice(&mut self, src: &[u8]) {
        Stage::extend_from_slice(self, src);
    }
    fn len(&self) -> usize {
        Stage::len(self)
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        Stage::as_mut_slice(self)
    }
}

pub trait Put: Sink {
    fn put_u8(&mut self, v: u8) {
        self.push(v);
    }

    fn put_i8(&mut self, v: i8) {
        self.push(v as u8);
    }

    fn put_u16(&mut self, v: u16) {
        self.extend_from_slice(&v.to_be_bytes());
    }

    fn put_i16(&mut self, v: i16) {
        self.extend_from_slice(&v.to_be_bytes());
    }

    fn put_u32(&mut self, v: u32) {
        self.extend_from_slice(&v.to_be_bytes());
    }

    fn put_i32(&mut self, v: i32) {
        self.extend_from_slice(&v.to_be_bytes());
    }

    fn put_u64(&mut self, v: u64) {
        self.extend_from_slice(&v.to_be_bytes());
    }

    fn put_i64(&mut self, v: i64) {
        self.extend_from_slice(&v.to_be_bytes());
    }

    fn put_f32(&mut self, v: f32) {
        self.extend_from_slice(&v.to_be_bytes());
    }

    fn put_f64(&mut self, v: f64) {
        self.extend_from_slice(&v.to_be_bytes());
    }

    fn put_bool(&mut self, v: bool) {
        self.push(if v { 1 } else { 0 });
    }

    fn put_short_str(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let n = bytes.len().min(u8::MAX as usize) as u8;
        self.push(n);
        self.extend_from_slice(&bytes[..n as usize]);
    }

    fn put_long_str(&mut self, s: &[u8]) {
        let n = s.len().min(u32::MAX as usize) as u32;
        self.put_u32(n);
        self.extend_from_slice(&s[..n as usize]);
    }

    fn reserve_u32(&mut self) -> usize {
        let pos = self.len();
        self.extend_from_slice(&[0u8; 4]);
        pos
    }

    fn finish_u32_at(&mut self, pos: usize, value: u32) {
        let bytes = value.to_be_bytes();
        self.as_mut_slice()[pos..pos + 4].copy_from_slice(&bytes);
    }
}

impl<S: Sink + ?Sized> Put for S {}
