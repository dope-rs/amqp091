use std::{cell, mem, num};

use o3::alloc;
use o3::depot::queue::round::{self, values};

use crate::service::cancel::layout::Layout;
use crate::service::cancel::pending::{Pending, reap};
use crate::service::cancel::{Lane, Plan, Signal};
use crate::state::channel::cancel::lanes;
use crate::state::channel::methods;
use crate::state::channels;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Active<'d, const ID: u8> {
    pub(super) identity: num::NonZero<u64>,
    lease: channels::Lease<'d, ID>,
}

pub(crate) struct Registry<'d, const ID: u8> {
    entries: values::Values<Active<'d, ID>, { lanes::Kind::COUNT }>,
    layout: Layout,
    cursor: cell::Cell<lanes::Kind>,
}

pub(crate) struct Retry<'a, 'd, const ID: u8> {
    inner: values::Snapshot<'a, Active<'d, ID>, { lanes::Kind::COUNT }, round::Turn>,
    active: Active<'d, ID>,
}

impl<'d, const ID: u8> Retry<'_, 'd, ID> {
    pub(crate) fn lease(&self) -> channels::Lease<'d, ID> {
        self.active.lease
    }

    pub(crate) fn complete(self) {
        let active = self.active;
        let _ = self.inner.pop_if(|current| current == active);
    }
}

impl<'d, const ID: u8> Active<'d, ID> {
    pub(super) fn lease(&self) -> channels::Lease<'d, ID> {
        self.lease
    }
}

impl<'d, const ID: u8> Registry<'d, ID> {
    pub(crate) fn try_new(plan: Plan) -> Result<Self, alloc::Error> {
        let lanes = values::Values::try_new(plan.entries() as usize, plan.queues()?)?;
        Ok(Self {
            entries: lanes,
            layout: plan.layout(),
            cursor: cell::Cell::new(lanes::Kind::Reservation),
        })
    }

    pub(super) fn activate<'a, L: Lane<'d, ID>>(
        &'a self,
        ch: channels::Index<'_, ID>,
        slot: lanes::Slot<L>,
        lease: channels::Lease<'d, ID>,
        id: L::Identity,
    ) -> Result<
        values::Snapshot<'a, Active<'d, ID>, { lanes::Kind::COUNT }, round::Place>,
        round::Bounds,
    > {
        let node = u32::from(ch.offset()) * self.layout.slots()
            + self.layout.offset(L::KIND, slot.raw())?;
        let slot = self.entries.try_slot(node as usize)?;
        let queue = self.lane(u32::from(ch.offset()), L::KIND);
        let place = slot.try_lane(queue as usize)?;
        slot.write(Active {
            identity: id.into(),
            lease,
        });
        Ok(place)
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.entries.group::<0>().active().is_some()
            || self.entries.group::<1>().active().is_some()
            || self.entries.group::<2>().active().is_some()
            || self.entries.group::<3>().active().is_some()
            || self.entries.group::<4>().active().is_some()
    }

    pub(crate) fn has_control(&self) -> bool {
        self.control().is_some()
    }

    fn control(&self) -> Option<Retry<'_, 'd, ID>> {
        let (inner, active) = self.entries.group::<5>().active()?;
        Some(Retry { inner, active })
    }

    pub(crate) fn retry(
        &self,
        index: channels::Index<'_, ID>,
        lease: channels::Lease<'d, ID>,
        method: methods::MethodKey,
    ) -> Result<(), round::Bounds> {
        let entry = self.activate(index, lanes::Slot::<lanes::Control>::ZERO, lease, method)?;
        let _ = entry.set(|_| Some(()));
        Ok(())
    }

    pub(crate) fn pop(&self, control: bool) -> Option<Signal<'_, 'd, ID>> {
        let mut kind = self.cursor.get();
        let mut left = lanes::Kind::COUNT;
        while left != 0 {
            if kind == lanes::Kind::Control
                && control
                && let Some(retry) = self.control()
            {
                self.cursor.set(Self::advance(kind));
                return Some(Signal::Control(retry));
            }
            let next = match kind {
                lanes::Kind::Reservation => self.entries.group::<0>().next(),
                lanes::Kind::Method => self.entries.group::<1>().next(),
                lanes::Kind::Consumer => self.entries.group::<2>().next(),
                lanes::Kind::Confirm => self.entries.group::<3>().next(),
                lanes::Kind::Publish => self.entries.group::<4>().next(),
                lanes::Kind::Control => None,
            };
            let Some(active) = next else {
                kind = Self::advance(kind);
                left -= 1;
                continue;
            };
            self.cursor.set(Self::advance(kind));
            return Some(Signal::Cancel {
                lease: active.lease,
                kind,
                identity: active.identity,
            });
        }
        None
    }

    pub(crate) fn reap_any<L: Lane<'d, ID>>(
        &self,
        reap: impl FnOnce(Pending<'d, L, ID>) -> reap::Decision,
    ) {
        let head = match L::KIND {
            lanes::Kind::Reservation => self.entries.group::<0>().active(),
            lanes::Kind::Method => self.entries.group::<1>().active(),
            lanes::Kind::Consumer => self.entries.group::<2>().active(),
            lanes::Kind::Confirm => self.entries.group::<3>().active(),
            lanes::Kind::Publish => self.entries.group::<4>().active(),
            lanes::Kind::Control => self.entries.group::<5>().active(),
        };
        let Some((turn, active)) = head else {
            return;
        };
        let Active {
            lease,
            identity: id,
        } = active;
        if matches!(reap(Pending::new(lease, id)), reap::Decision::Keep) {
            return;
        }
        let _ = turn.pop_if(|current| current == active);
    }

    pub(crate) fn try_reap<L: Lane<'d, ID>, R>(
        &self,
        ch: channels::Index<'_, ID>,
        lease: channels::Lease<'d, ID>,
        reap: impl FnOnce(Pending<'d, L, ID>) -> Option<R>,
    ) -> Result<Option<R>, round::Bounds> {
        let queue = self.lane(u32::from(ch.offset()), L::KIND);
        let Some((front, active)) = self.entries.try_lane(queue as usize)?.front() else {
            return Ok(None);
        };
        let Active {
            lease: held,
            identity: id,
        } = active;
        if lease != held {
            return Ok(None);
        }
        let Some(result) = reap(Pending::new(held, id)) else {
            return Ok(None);
        };
        let _ = front.pop_if(|current| current == active);
        Ok(Some(result))
    }

    const fn lane(&self, ch: u32, kind: lanes::Kind) -> u32 {
        ch * lanes::Kind::COUNT as u32 + kind.index() as u32
    }

    const fn advance(kind: lanes::Kind) -> lanes::Kind {
        match kind {
            lanes::Kind::Reservation => lanes::Kind::Method,
            lanes::Kind::Method => lanes::Kind::Consumer,
            lanes::Kind::Consumer => lanes::Kind::Confirm,
            lanes::Kind::Confirm => lanes::Kind::Publish,
            lanes::Kind::Publish => lanes::Kind::Control,
            lanes::Kind::Control => lanes::Kind::Reservation,
        }
    }
}

const _: () = assert!(mem::size_of::<Active<'static, 0>>() == 2 * mem::size_of::<u64>());
