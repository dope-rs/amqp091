use dope::core::driver::schedule::ready::wake::store;
use o3::depot::arena::linked::fifo;

use crate::failure::wire::turn::Consumer;
use crate::protocol::topology::records::consumer::Record;
use crate::protocol::wire::text::short;
use crate::state::channel::consumer::delivery;
use crate::state::layout;

#[repr(C)]
pub(in crate::state::channel::consumer) struct Slot<'b, 'd, const ID: u8> {
    pub(in crate::state::channel::consumer) spec: Record<'b, 'd, ID>,
    pub(in crate::state::channel::consumer) queue: fifo::Fifo<delivery::Tag>,
    pub(in crate::state::channel::consumer) waker: store::Entry<'d>,
    pub(in crate::state::channel::consumer) status: Consumer,
}

impl<'b, 'd, const ID: u8> Slot<'b, 'd, ID> {
    pub(in crate::state::channel::consumer) fn pending(spec: Record<'b, 'd, ID>) -> Self {
        Self {
            spec,
            queue: fifo::Fifo::new(),
            waker: store::Entry::empty(),
            status: Consumer::Pending,
        }
    }

    pub(in crate::state::channel::consumer) fn tag(&self) -> short::Short<'_> {
        self.spec.tag()
    }

    pub(in crate::state::channel::consumer) const fn buffered(&self) -> u64 {
        self.queue.len() as u64
    }
}

impl<const ID: u8> AsRef<str> for Slot<'_, '_, ID> {
    fn as_ref(&self) -> &str {
        self.tag().value()
    }
}

const _: () = layout::Layout::u64_words(8).prove::<Slot<'_, '_, 0>>();
