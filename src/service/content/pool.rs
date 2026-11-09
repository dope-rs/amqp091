use std::mem;

use o3::buffer::{hold, pool};
use o3::depot::arena::pool::recycle;
use o3::depot::capacity;
use o3::{alloc, depot};
use recycle::release;

use crate::failure::wire;
use crate::protocol::method::basic;
use crate::service::content::wait::{resource, wake};
use crate::service::content::{entry, pending};
use crate::state::channel::consumer::delivery;
use crate::state::channel::returns;

pub(in crate::service) struct Owners {
    delivery: capacity::Count,
    returned: capacity::Count,
    pub(in crate::service) fragments: capacity::Count,
    owners: capacity::Count,
}

pub(crate) struct Pool<'b, 'd> {
    pub(super) compact: &'b pool::Pool,
    delivery: release::Store<entry::Entry<'b, 'd>, wake::Wake<'b, 'd>>,
    returned: release::Store<entry::Entry<'b, 'd>, wake::Wake<'b, 'd>>,
    pub(super) fragments: &'b depot::raw::Arena<hold::Retained<'d>>,
    owners: capacity::Count,
}

impl Owners {
    pub(in crate::service) fn try_new(
        delivery: capacity::Count,
        returned: capacity::Count,
        frags: capacity::Count,
    ) -> Result<Self, wire::Config> {
        let sum = u64::from(delivery.raw()) + u64::from(returned.raw()) + u64::from(frags.raw());
        let owners = match u32::try_from(sum) {
            Ok(owners) => capacity::Count::new(owners),
            Err(error) => {
                return Err(wire::Config::ContentOwners {
                    delivery,
                    returned,
                    fragments: frags,
                    error,
                });
            }
        };
        Ok(Self {
            delivery,
            returned,
            fragments: frags,
            owners,
        })
    }
}

impl<'b, 'd> Pool<'b, 'd> {
    pub(in crate::service) fn try_new(
        owners: Owners,
        compact: &'b pool::Pool,
        segments: &'b depot::raw::Arena<hold::Retained<'d>>,
        wake: wake::Wake<'b, 'd>,
    ) -> Result<Self, alloc::Error> {
        let Owners {
            delivery,
            returned,
            fragments: _,
            owners,
        } = owners;
        Ok(Self {
            compact,
            delivery: release::Store::try_init(delivery, wake, resource::Entry::Delivery)?,
            returned: release::Store::try_init(returned, wake, resource::Entry::Returned)?,
            fragments: segments,
            owners,
        })
    }

    pub(crate) fn owners(&self) -> capacity::Count {
        self.owners
    }

    pub(crate) fn deliver<'input, 'r, 'c>(
        &'c self,
        method: basic::Deliver<'input>,
        route: delivery::Route<'r, 'c, 'b, 'd>,
    ) -> Result<pending::Pending<'r, 'c, 'b, 'd>, resource::Slot> {
        let slot = self
            .delivery
            .vacancy()
            .ok_or(resource::Slot::Delivery)?
            .reserve();
        Ok(pending::Pending::delivery(slot, method, route))
    }

    pub(crate) fn accept<'input, 'r, 'c>(
        &'c self,
        method: basic::Return<'input>,
        route: returns::Vacant<'r, 'c, 'b, 'd>,
    ) -> Result<pending::Pending<'r, 'c, 'b, 'd>, resource::Slot> {
        let slot = self
            .returned
            .vacancy()
            .ok_or(resource::Slot::Returned)?
            .reserve();
        Ok(pending::Pending::returned(slot, method, route))
    }
}

const _: () = assert!(
    mem::size_of::<Result<pending::Pending<'_, '_, '_, '_>, resource::Slot>>()
        == mem::size_of::<Option<pending::Pending<'_, '_, '_, '_>>>()
);
const _: () = assert!(mem::size_of::<Owners>() == 4 * mem::size_of::<u32>());
