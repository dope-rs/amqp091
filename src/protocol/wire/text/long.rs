use crate::failure::{self, wire};

mod raw;

#[derive(Clone, Copy)]
pub struct Long<'a>(&'a [u8]);

impl<'a> Long<'a> {
    pub fn new(value: &'a [u8]) -> Result<Self, failure::Error> {
        Self::check_len(value.len())?;
        Ok(Self(value))
    }

    fn check_len(len: usize) -> Result<(), failure::Error> {
        if len > u32::MAX as usize {
            return Err(failure::Error::Encoding(wire::Encode::Length {
                kind: wire::Encoding::Long,
                actual: len,
                max: u32::MAX as usize,
            }));
        }
        Ok(())
    }

    pub fn value(self) -> &'a [u8] {
        self.0
    }

    pub fn len(self) -> u32 {
        self.0.len() as u32
    }

    pub fn is_empty(self) -> bool {
        self.0.is_empty()
    }
}

pub struct Buf(Vec<u8>);

impl Buf {
    pub fn new(value: Vec<u8>) -> Result<Self, failure::Error> {
        Long::new(&value)?;
        Ok(Self(value))
    }

    pub fn as_long(&self) -> Long<'_> {
        Long(self.0.as_slice())
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        self.0.as_mut_slice()
    }
}
