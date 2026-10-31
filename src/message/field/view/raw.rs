use crate::message::field::view::tag::Tag;

/// # Safety
/// The cursor must be at a nonempty value-tag boundary established by its
/// immutable body's complete validation, after any table key was consumed.
pub(super) unsafe fn tag(input: &mut &[u8]) -> Tag {
    let source = *input;
    // SAFETY: validation matched this byte against constants derived from Tag's
    // discriminants and checked its complete payload. The sole caller follows
    // those same widths from an immutable admitted table or array. Exact clone,
    // compaction and nested slicing retain its bytes and value boundaries.
    // Tag has u8 layout and every admitted byte is its own discriminant.
    let value = unsafe { source.as_ptr().cast::<Tag>().read() };
    // SAFETY: the same admission includes this complete tag byte.
    *input = unsafe { source.get_unchecked(1..) };
    value
}
