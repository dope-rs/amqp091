use std::slice;

use o3::alloc::VecExt as _;

use crate::failure;
use crate::failure::wire::{Allocation, Encoding};
use crate::message::field;
use crate::protocol::wire;
use crate::protocol::wire::text::short;

#[derive(Default)]
pub struct Table {
    entries: Vec<(short::Buf, field::Value)>,
    encoded_len: u32,
}

impl Table {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            encoded_len: 0,
        }
    }

    /// Preserves the checked total encoded extent when adding or replacing an entry.
    pub fn insert(
        &mut self,
        key: impl AsRef<str>,
        value: field::Value,
    ) -> Result<&mut Self, failure::Error> {
        let key = short::Short::new(key.as_ref())?;
        let new_len = entry_len(key.len(), &value)?;
        match self
            .entries
            .binary_search_by(|entry| entry.0.as_ref().cmp(key.value()))
        {
            Ok(index) => {
                let old = &mut self.entries[index].1;
                let old_len = 1 + u32::from(key.len()) + old.encoded_len();
                let base = self.encoded_len - old_len;
                let total = field::sum(Encoding::FieldTable, base, new_len)?;
                *old = value;
                self.encoded_len = total;
            }
            Err(index) => {
                let total = field::sum(Encoding::FieldTable, self.encoded_len, new_len)?;
                if self.entries.len() == self.entries.capacity() {
                    let grow = self.entries.capacity().max(1);
                    self.entries
                        .try_grow(grow)
                        .map_err(|error| failure::Error::Allocation {
                            target: Allocation::FieldTable,
                            error,
                        })?;
                }
                let key = short::Buf::try_copy(key, short::Resource::Short)?;
                self.entries.insert(index, (key, value));
                self.encoded_len = total;
            }
        }
        Ok(self)
    }

    pub(crate) fn encoded_len(&self) -> u32 {
        self.encoded_len
    }

    pub(super) fn entries(&self) -> slice::Iter<'_, (short::Buf, field::Value)> {
        self.entries.iter()
    }

    pub(crate) fn encode<S: wire::Put>(&self, out: &mut S) {
        out.put_u32(self.encoded_len);
        for (key, value) in &self.entries {
            out.put_short(key.as_short());
            value.encode(out);
        }
    }
}

fn entry_len(key_len: u8, value: &field::Value) -> Result<u32, failure::Error> {
    field::sum(
        Encoding::FieldTable,
        1 + u32::from(key_len),
        value.encoded_len(),
    )
}
