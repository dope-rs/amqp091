use super::super::*;

pub struct Outbound {
    buffer: Lease<'static>,
    overflowed: bool,
}

impl Outbound {
    pub(in crate::protocol) fn new(buffer: Lease<'_>) -> Self {
        Self {
            // SAFETY: every outbound lease is owned by a connector queue whose
            // lifetime is bounded by the driver-scoped `AmqpPort` that owns the
            // pinned pool. `AmqpPort` drops the queue before the pool.
            buffer: unsafe { transmute::<Lease<'_>, Lease<'static>>(buffer) },
            overflowed: false,
        }
    }

    pub(in crate::protocol) fn overflowed(&self) -> bool {
        self.overflowed
    }
}

impl AsRef<[u8]> for Outbound {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl Sink for Outbound {
    fn push(&mut self, byte: u8) {
        if self.buffer.try_push(byte).is_err() {
            self.overflowed = true;
        }
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        if self.buffer.try_extend_from_slice(src).is_err() {
            self.overflowed = true;
        }
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }
}
