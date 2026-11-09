use std::{num, ptr, str};

use o3::buffer::{self, storage};

use crate::failure::{self, wire};
use crate::message::property::{self, Field, Key, TEXT_FIELDS, source};

const CAPACITY: usize = (TEXT_FIELDS - 1) * (u8::MAX as usize + 1) + 1;

// SAFETY: Source retains one immutable slice captured at construction; every
// projection returns that same range and contents without exposing mutation.
unsafe impl buffer::raw::Source for source::Source<'_, '_> {}

pub(super) fn text<S: buffer::raw::Source>(
    source: &property::Text<S>,
    field: Field,
) -> Option<&str> {
    let start = source.keys[field as usize].0?.get() as usize;
    let text = source.bytes.as_ref();
    // SAFETY: only property admission creates keys, after validating this owner's
    // prefix, complete range and UTF-8. This shared borrow prevents text mutation;
    // Source preserves the same range and contents while borrowed.
    unsafe {
        let len = usize::from(*text.get_unchecked(start - 1));
        Some(str::from_utf8_unchecked(
            text.get_unchecked(start..start + len),
        ))
    }
}

impl property::Text<storage::Owned> {
    pub(in crate::message) fn set_text(
        &mut self,
        field: Field,
        value: &str,
    ) -> Result<(), failure::Error> {
        if let Some(offset) = value.find('\0') {
            return Err(failure::Error::Protocol(wire::Fault::TextNul {
                kind: wire::Encoding::Short,
                offset,
            }));
        }
        if value.len() > u8::MAX as usize {
            return Err(failure::Error::Encoding(wire::Encode::Length {
                kind: wire::Encoding::Short,
                actual: value.len(),
                max: u8::MAX as usize,
            }));
        }
        let old = self.keys[field as usize].0.map(|key| {
            let start = key.get() as usize;
            // SAFETY: each private key retains its complete record in this owner.
            let len = unsafe { *self.bytes.get_unchecked(start - 1) };
            (start, usize::from(len))
        });
        if let Some((start, len)) = old
            && len == value.len()
        {
            // SAFETY: the retained record is initialized and exactly value.len()
            // bytes long; this exclusive borrow prevents overlapping input.
            unsafe { self.bytes.get_unchecked_mut(start..start + len) }
                .copy_from_slice(value.as_bytes());
            return Ok(());
        }
        let removed = old.map_or(0, |(_, len)| len + 1);
        let used = self.bytes.len();
        let retained = used - removed;
        let need = retained + value.len() + 1;
        let capacity = self.bytes.capacity();
        if need > capacity {
            let capacity = need.max(capacity * 2).min(CAPACITY);
            let Some(capacity) = num::NonZero::new(capacity as u32) else {
                unreachable!("capacity")
            };
            self.bytes
                .try_reserve(capacity)
                .map_err(|error| failure::Error::Allocation {
                    target: wire::Allocation::Property,
                    error: error.into(),
                })?;
        }
        if let Some((start, len)) = old {
            let end = start + len;
            if end < used {
                let bytes = self.bytes.as_mut_ptr();
                // SAFETY: reservation preserved the initialized records. Removing
                // this record moves its complete suffix left within the same
                // exclusive allocation; copy permits these ranges to overlap.
                unsafe { ptr::copy(bytes.add(end), bytes.add(start - 1), used - end) };
                for key in &mut self.keys {
                    if let Some(offset) = key.0
                        && offset.get() as usize > start
                    {
                        *key = Key::new(offset.get() - removed as u32);
                    }
                }
            }
        }
        let prefix = [value.len() as u8];
        // SAFETY: short-string admission bounds this exact source to 1..=256 bytes.
        // Reservation covers retained + source; retained never exceeds the old length.
        // Both immutable sources are disjoint from this exclusively owned allocation.
        unsafe {
            let len = num::NonZero::new_unchecked(value.len() as u32 + 1);
            buffer::raw::Slices::new(len, [prefix.as_slice(), value.as_bytes()])
                .replace(&mut self.bytes, retained as u32);
        }
        self.keys[field as usize] = Key::new(retained as u32 + 1);
        Ok(())
    }
}

const _: () = assert!(CAPACITY < u32::MAX as usize);
