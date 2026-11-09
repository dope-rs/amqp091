use o3::depot::queue::round;

use crate::failure::{self, wire};
use crate::state::channel::cancel::lanes;
use crate::state::limits;

#[derive(Clone, Copy)]
pub(super) struct Layout {
    method_end: u32,
    consumer_end: u32,
    confirm_end: u32,
}

impl Layout {
    pub(super) fn try_new(limits: limits::Limits) -> Result<Self, failure::Error> {
        let method = limits.methods.user.raw();
        let count = limits.consumers.raw();
        let consumer = method.checked_add(count).ok_or_else(|| {
            failure::Error::InvalidConfig(wire::Config::CancellationConsumer {
                methods: method,
                consumers: count,
            })
        })?;
        let count = limits.confirms.raw();
        let confirm = consumer.checked_add(count).ok_or_else(|| {
            failure::Error::InvalidConfig(wire::Config::CancellationConfirm {
                consumer_end: consumer,
                confirms: count,
            })
        })?;
        confirm.checked_add(3).ok_or_else(|| {
            failure::Error::InvalidConfig(wire::Config::CancellationSlots {
                confirm_end: confirm,
            })
        })?;
        Ok(Self {
            method_end: method,
            consumer_end: consumer,
            confirm_end: confirm,
        })
    }

    pub(super) const fn slots(self) -> u32 {
        self.confirm_end + 3
    }

    pub(super) fn offset(self, kind: lanes::Kind, slot: u32) -> Result<u32, round::Bounds> {
        let (base, end) = match kind {
            lanes::Kind::Reservation => (self.confirm_end + 1, self.confirm_end + 2),
            lanes::Kind::Method => (0, self.method_end),
            lanes::Kind::Consumer => (self.method_end, self.consumer_end),
            lanes::Kind::Confirm => (self.consumer_end, self.confirm_end),
            lanes::Kind::Publish => (self.confirm_end, self.confirm_end + 1),
            lanes::Kind::Control => (self.confirm_end + 2, self.confirm_end + 3),
        };
        if slot >= end - base {
            return Err(round::Bounds::Slot {
                index: slot as usize,
                capacity: (end - base) as usize,
            });
        }
        Ok(base + slot)
    }
}

const _: () = assert!(size_of::<Layout>() == 3 * size_of::<u32>());
