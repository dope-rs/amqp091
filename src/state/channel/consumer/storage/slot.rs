use dope::core::driver::schedule::ready::wake::store;

use crate::protocol::topology::records::consumer::Record;
use crate::protocol::wire::text::short;
use crate::state::channel::consumer::delivery::queue::Queue;
use crate::state::channel::consumer::storage::status::Status;
use crate::state::layout;

#[repr(C)]
pub(in crate::state::channel::consumer) struct Slot<'b, 'd, const ID: u8> {
    pub(in crate::state::channel::consumer) spec: Record<'b, 'd, ID>,
    pub(in crate::state::channel::consumer) queue: Queue,
    pub(in crate::state::channel::consumer) waker: store::Entry<'d>,
    pub(in crate::state::channel::consumer) status: Status,
}

impl<'b, 'd, const ID: u8> Slot<'b, 'd, ID> {
    pub(in crate::state::channel::consumer) fn pending(spec: Record<'b, 'd, ID>) -> Self {
        Self {
            spec,
            queue: Queue::empty(),
            waker: store::Entry::empty(),
            status: Status::Pending,
        }
    }

    pub(in crate::state::channel::consumer) fn tag(&self) -> short::Short<'_> {
        self.spec.tag()
    }

    pub(in crate::state::channel::consumer) const fn buffered(&self) -> u64 {
        self.queue.len()
    }
}

impl<const ID: u8> AsRef<str> for Slot<'_, '_, ID> {
    fn as_ref(&self) -> &str {
        self.tag().value()
    }
}

const _: () = layout::Layout::u64_words(8).prove::<Slot<'_, '_, 0>>();
