use o3::alloc::{self, VecExt as _};

use crate::failure::{self, wire};

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
    pub(crate) fn try_concat(
        parts: &[&[u8]],
        target: wire::Allocation,
    ) -> Result<Self, failure::Error> {
        let len = parts
            .iter()
            .try_fold(0usize, |len, part| len.checked_add(part.len()))
            .ok_or(failure::Error::Allocation {
                target,
                error: alloc::Error::Overflow,
            })?;
        Long::check_len(len)?;
        let mut value =
            Vec::try_new(len).map_err(|error| failure::Error::Allocation { target, error })?;
        for part in parts {
            value.extend_from_slice(part);
        }
        Ok(Self(value))
    }

    pub fn new(value: Vec<u8>) -> Result<Self, failure::Error> {
        Long::new(&value)?;
        Ok(Self(value))
    }

    pub fn as_long(&self) -> Long<'_> {
        Long(self.0.as_slice())
    }
}
