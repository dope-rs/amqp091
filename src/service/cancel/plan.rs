use std::num;

use o3::alloc;

use crate::failure::{self, wire};
use crate::service::cancel::layout::Layout;
use crate::state::channel::cancel::lanes;
use crate::state::limits;

#[derive(Clone, Copy)]
pub(crate) struct Plan {
    channels: num::NonZero<u32>,
    layout: Layout,
}

impl Plan {
    pub(crate) fn try_new(channels: u32, limits: limits::Limits) -> Result<Self, failure::Error> {
        let channels = num::NonZero::<u32>::new(channels)
            .ok_or_else(|| failure::Error::InvalidConfig(wire::Config::CancellationChannelZero))?;
        let layout = Layout::try_new(limits)?;
        let slots = layout.slots();
        if channels.get() > u32::MAX / slots {
            return Err(failure::Error::InvalidConfig(
                wire::Config::CancellationEntryCapacity {
                    channels: channels.get(),
                    slots,
                },
            ));
        }
        Ok(Self { channels, layout })
    }

    pub(super) const fn entries(self) -> u32 {
        self.channels.get() * self.layout.slots()
    }

    pub(super) fn queues(self) -> Result<num::NonZero<usize>, alloc::Error> {
        (self.channels.get() as usize)
            .checked_mul(lanes::Kind::COUNT)
            .and_then(num::NonZero::<usize>::new)
            .ok_or(alloc::Error::Overflow)
    }

    pub(super) const fn layout(self) -> Layout {
        self.layout
    }
}

const _: () = assert!(size_of::<Plan>() == 4 * size_of::<u32>());
