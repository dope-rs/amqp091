use crate::wire::Sink;

pub struct Bits;

impl Bits {
    pub fn pack<S: Sink>(out: &mut S, flags: &[bool]) {
        let mut byte = 0u8;
        let mut count = 0u8;
        for &b in flags {
            if b {
                byte |= 1 << count;
            }
            count += 1;
            if count == 8 {
                out.push(byte);
                byte = 0;
                count = 0;
            }
        }
        if count != 0 {
            out.push(byte);
        }
    }
}
