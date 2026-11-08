use std::{cell, mem, num};

use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::ready;
use dope::core::driver::{self, route};
use dope::manifold::client::conn;
use dope::net::link::egress::metadata;
use o3::buffer::resident;
use o3::depot::capacity;
use o3::depot::queue::round::{self, phase, values};
use o3::mem::credit;
use o3::{alloc, depot};

use crate::failure::{self, wire};
use crate::service::content::pending;
use crate::service::content::wait::release::Release;
use crate::service::content::wait::resource::{self, Resource};

#[derive(Clone, Copy, PartialEq, Eq)]
struct Waiting<'d> {
    target: ready::Retained<'d>,
    generation: route::Epoch,
    resource: mem::Discriminant<Resource>,
    demand: num::NonZero<usize>,
}

pub(in crate::service::content::wait) struct Store<'d> {
    owner: driver::Reference<'d>,
    body: credit::Ledger,
    completed_body: cell::Cell<usize>,
    fragments: capacity::Count,
    ready: cell::Cell<u32>,
    owners: cell::Cell<u32>,
    resident: resident::Budget<'d>,
    waits: values::Values<Waiting<'d>, { resource::Kind::COUNT }, { resource::Kind::COUNT }>,
}

impl<'d> Store<'d> {
    pub(in crate::service::content::wait) fn try_new(
        capacity: metadata::LaneCount,
        bytes: usize,
        fragments: capacity::Count,
        resident: resident::Budget<'d>,
        owner: driver::Reference<'d>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            owner,
            body: credit::Ledger::new(bytes),
            completed_body: cell::Cell::new(0),
            fragments,
            ready: cell::Cell::new(0),
            owners: cell::Cell::new(0),
            resident,
            waits: values::Values::try_fixed(capacity.count().get())?,
        })
    }

    pub(in crate::service::content::wait) const fn body(&self) -> &credit::Ledger {
        &self.body
    }

    pub(in crate::service::content::wait) const fn resident(&self) -> resident::Budget<'d> {
        self.resident
    }

    pub(in crate::service::content::wait) fn complete(
        &self,
        body: usize,
        fragments: u32,
        resident: bool,
    ) {
        // Completed payloads retain disjoint ledger debits until recycling.
        // Their entries and distinct nodes stay within the admitted owner count.
        self.completed_body.set(self.completed_body.get() + body);
        self.ready.set(self.ready.get() + fragments);
        self.owners.set(self.owners.get() + u32::from(resident));
    }

    pub(in crate::service::content::wait) fn body_exhausted(
        &self,
        error: credit::Error,
    ) -> pending::Error {
        let (need, spare) = error.into_parts();
        // The credit error preserves need > spare; completed owners can release these bytes.
        if need.get() - spare <= self.completed_body.get() {
            pending::Error::Retry(Resource::Body(error))
        } else {
            pending::Error::Failure(failure::Error::Capacity(wire::Capacity::BodyBudget(error)))
        }
    }

    pub(in crate::service::content::wait) fn can_release(&self) -> bool {
        self.owners.get() != 0
    }

    pub(in crate::service::content::wait) fn fragment(&self) -> pending::Error {
        if self.ready.get() == 0 {
            pending::Error::Failure(failure::Error::Capacity(wire::Capacity::BodyFragments {
                capacity: self.fragments,
            }))
        } else {
            pending::Error::Retry(Resource::Fragment)
        }
    }

    pub(in crate::service::content::wait) fn register<const ID: u8>(
        &self,
        lane: conn::Lane<'d, KeyTag<ID>>,
        epoch: route::Epoch,
        resource: Resource,
        target: ready::Target<'d>,
    ) -> Result<(), round::Bounds> {
        let index = lane.slot().index().get() as usize;
        let slot = self.waits.try_slot(index)?;
        let demand = match resource {
            Resource::Body(error) | Resource::Resident(error) | Resource::Staging(error) => {
                error.into_parts().0
            }
            Resource::Delivery | Resource::Returned | Resource::Fragment | Resource::Route => {
                num::NonZero::<usize>::MIN
            }
        };
        let waiting = Waiting {
            target: target.key(),
            generation: epoch,
            resource: mem::discriminant(&resource),
            demand,
        };
        if slot.get() == Some(waiting) {
            return Ok(());
        }
        match resource.kind() {
            Some(kind) => slot.lane(kind.index()).push(waiting),
            None => slot.write(waiting),
        }
        Ok(())
    }

    pub(in crate::service::content::wait) fn cancel<const ID: u8>(
        &self,
        lane: conn::Lane<'d, KeyTag<ID>>,
        epoch: route::Epoch,
    ) -> Result<(), round::Bounds> {
        let index = lane.slot().index().get() as usize;
        let slot = self.waits.try_slot(index)?;
        let Some(waiting) = slot.get() else {
            return Ok(());
        };
        if waiting.generation != epoch {
            return Ok(());
        }
        let _ = slot.take();
        Ok(())
    }

    pub(in crate::service::content::wait) fn wake_route<const ID: u8>(
        &self,
        lane: conn::Lane<'d, KeyTag<ID>>,
        epoch: route::Epoch,
    ) -> Result<(), round::Bounds> {
        let index = lane.slot().index().get() as usize;
        let slot = self.waits.try_slot(index)?;
        let Some(waiting) = slot.get() else {
            return Ok(());
        };
        if waiting.generation != epoch || waiting.resource != mem::discriminant(&Resource::Route) {
            return Ok(());
        }
        let _ = slot.take();
        waiting.target.wake(self.owner);
        Ok(())
    }

    pub(in crate::service::content::wait) fn released(&self, release: Release) {
        if release.complete {
            // Recycling consumes this completed payload's exact return authority.
            self.completed_body
                .set(self.completed_body.get() - release.body);
            self.ready.set(self.ready.get() - release.fragments);
            self.owners
                .set(self.owners.get() - u32::from(release.resident));
        }
        if let Some(entry) = release.entry
            && let Some(target) = self.take_one(entry.kind())
        {
            target.wake(self.owner);
        }
        if release.body != 0 {
            let free = self.body.available();
            self.wake(resource::Kind::Body, free);
        }
        if release.resident {
            // Staging uses a separate budget; only its allocator can re-admit.
            // Retry each retained generation once after an owner releases.
            while let Some(target) = self.take_one(resource::Kind::Staging) {
                target.wake(self.owner);
            }
            if self.owners.get() == 0 {
                while let Some(target) = self.take_one(resource::Kind::Resident) {
                    target.wake(self.owner);
                }
            } else {
                self.wake(resource::Kind::Resident, self.resident.available());
            }
        }
        for _ in 0..release.fragments {
            let Some(target) = self.take_one(resource::Kind::Fragment) else {
                break;
            };
            target.wake(self.owner);
        }
    }

    fn take_one(&self, kind: resource::Kind) -> Option<ready::Retained<'d>> {
        let waiting = self.waits.lane(kind.index()).pop_front()?;
        Some(waiting.target)
    }

    fn wake(&self, kind: resource::Kind, mut free: usize) {
        while let Some((target, left)) = self.take_fit(kind, free) {
            target.wake(self.owner);
            free = left;
        }
    }

    fn take_fit(&self, kind: resource::Kind, free: usize) -> Option<(ready::Retained<'d>, usize)> {
        use o3::depot::raw::Admit as _;

        let queue = self.waits.lane(kind.index());
        // SAFETY: this thread-bound store invokes no callbacks or wakeups until this
        // traversal ends; the retained head alone changes this exact topology.
        let mut front = unsafe { depot::raw::Proof::<_, phase::Ready>::new(queue) }.admit()?;
        let first = front.index();
        loop {
            let demand = front.get().demand.get();
            if demand <= free {
                return Some((front.pop().target, free - demand));
            }
            let next = front.rotate();
            if next.index() == first {
                return None;
            }
            front = next;
        }
    }
}

const _: () = assert!(mem::size_of::<route::Epoch>() == mem::size_of::<usize>());
const _: () = assert!(mem::align_of::<route::Epoch>() == mem::align_of::<usize>());
const _: () = assert!(mem::size_of::<Waiting<'static>>() == 4 * mem::size_of::<usize>());
const _: () =
    assert!(mem::size_of::<Option<Waiting<'static>>>() == mem::size_of::<Waiting<'static>>());
