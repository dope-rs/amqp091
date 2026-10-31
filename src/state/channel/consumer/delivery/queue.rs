use o3::depot::arena::linked::fifo;

use crate::state::channel::consumer::delivery;

#[repr(transparent)]
pub(in crate::state::channel::consumer) struct Queue {
    pub(super) raw: fifo::Fifo<delivery::Tag>,
}

impl Queue {
    pub(in crate::state::channel::consumer) const fn empty() -> Self {
        Self {
            raw: fifo::Fifo::new(),
        }
    }

    pub(in crate::state::channel::consumer) const fn len(&self) -> u64 {
        self.raw.len() as u64
    }
}

const _: () = assert!(std::mem::size_of::<Queue>() == 3 * std::mem::size_of::<u32>());
