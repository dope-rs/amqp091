use super::sink::Sink;

pub trait Put: Sink {
    fn put_u8(&mut self, value: u8) {
        self.push(value);
    }

    fn put_i8(&mut self, value: i8) {
        self.push(value as u8);
    }

    fn put_u16(&mut self, value: u16) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn put_i16(&mut self, value: i16) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u32(&mut self, value: u32) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn put_i32(&mut self, value: i32) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u64(&mut self, value: u64) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn put_i64(&mut self, value: i64) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn put_f32(&mut self, value: f32) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn put_f64(&mut self, value: f64) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn put_bool(&mut self, value: bool) {
        self.push(if value { 1 } else { 0 });
    }

    fn put_bits(&mut self, flags: &[bool]) {
        let mut byte = 0u8;
        let mut count = 0u8;
        for &flag in flags {
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

    fn put_short_str(&mut self, value: &str) {
        let bytes = value.as_bytes();
        let len = bytes.len().min(u8::MAX as usize) as u8;
        self.push(len);
        self.extend_from_slice(&bytes[..len as usize]);
    }

    fn put_long_str(&mut self, value: &[u8]) {
        let len = value.len().min(u32::MAX as usize) as u32;
        self.put_u32(len);
        self.extend_from_slice(&value[..len as usize]);
    }

    fn reserve_u32(&mut self) -> usize {
        let position = self.len();
        self.extend_from_slice(&[0u8; 4]);
        position
    }

    fn finish_u32_at(&mut self, position: usize, value: u32) {
        let bytes = value.to_be_bytes();
        self.as_mut_slice()[position..position + 4].copy_from_slice(&bytes);
    }
}

impl<S: Sink + ?Sized> Put for S {}
