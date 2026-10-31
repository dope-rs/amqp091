use std::str;

use o3::buffer;

use crate::message::property::{self, source};

// SAFETY: Source retains one immutable slice captured at construction; every
// projection returns that same range and contents without exposing mutation.
unsafe impl buffer::raw::Source for source::Source<'_, '_> {}

pub(super) fn text<S: buffer::raw::Source>(
    source: &property::Text<S>,
    field: usize,
) -> Option<&str> {
    let start = source.keys[field].0?.get() as usize;
    let text = source.bytes.as_ref();
    // SAFETY: only property admission creates keys, after validating this owner's
    // prefix, complete range and UTF-8. Text exposes no owner mutation or key
    // replacement; Source preserves that same range and contents while borrowed.
    unsafe {
        let len = usize::from(*text.get_unchecked(start - 1));
        Some(str::from_utf8_unchecked(
            text.get_unchecked(start..start + len),
        ))
    }
}
