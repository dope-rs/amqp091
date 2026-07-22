use super::super::*;

pub(in crate::protocol) struct ConfirmTracker<'d> {
    pub(in crate::protocol) next_tag: u64,
    pub(in crate::protocol) slots: Box<[ConfirmSlot<'d>]>,
    pub(in crate::protocol) len: usize,
    pub(in crate::protocol) head: u32,
    pub(in crate::protocol) tail: u32,
}

pub(in crate::protocol) struct PendingPublish<'d> {
    pub(in crate::protocol) tag: u64,
    pub(in crate::protocol) waker: WakeSlot<'d>,
    pub(in crate::protocol) outcome: Option<Result<PublishConfirm, Error>>,
    pub(in crate::protocol) detached: bool,
}

pub(in crate::protocol) struct ConfirmSlot<'d> {
    pub(in crate::protocol) pending: Option<PendingPublish<'d>>,
    pub(in crate::protocol) prev: u32,
    pub(in crate::protocol) next: u32,
}

impl<'d> ConfirmTracker<'d> {
    pub(in crate::protocol) fn new(capacity: usize) -> Self {
        Self {
            next_tag: 0,
            slots: (0..capacity)
                .map(|_| ConfirmSlot {
                    pending: None,
                    prev: NONE,
                    next: NONE,
                })
                .collect(),
            len: 0,
            head: NONE,
            tail: NONE,
        }
    }

    pub(in crate::protocol) fn index(&self, tag: u64) -> Option<usize> {
        let tag = usize::try_from(tag).ok()?.checked_sub(1)?;
        (!self.slots.is_empty()).then(|| tag % self.slots.len())
    }

    pub(in crate::protocol) fn get_mut(&mut self, tag: u64) -> Option<&mut PendingPublish<'d>> {
        let index = self.index(tag)?;
        self.slots[index]
            .pending
            .as_mut()
            .filter(|pending| pending.tag == tag)
    }

    pub(in crate::protocol) fn clear(&mut self) {
        self.next_tag = 0;
        self.len = 0;
        self.head = NONE;
        self.tail = NONE;
        for slot in &mut self.slots {
            slot.pending = None;
            slot.prev = NONE;
            slot.next = NONE;
        }
    }

    pub(in crate::protocol) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(in crate::protocol) fn link(&mut self, index: usize) {
        self.slots[index].prev = self.tail;
        self.slots[index].next = NONE;
        if self.tail == NONE {
            self.head = index as u32;
        } else {
            self.slots[self.tail as usize].next = index as u32;
        }
        self.tail = index as u32;
    }

    pub(in crate::protocol) fn unlink(&mut self, index: usize) {
        let prev = self.slots[index].prev;
        let next = self.slots[index].next;
        if prev == NONE {
            self.head = next;
        } else {
            self.slots[prev as usize].next = next;
        }
        if next == NONE {
            self.tail = prev;
        } else {
            self.slots[next as usize].prev = prev;
        }
        self.slots[index].prev = NONE;
        self.slots[index].next = NONE;
    }

    pub(in crate::protocol) fn linked(&self, index: usize) -> bool {
        self.slots[index].prev != NONE
            || self.slots[index].next != NONE
            || self.head == index as u32
    }

    pub(in crate::protocol) fn remove(&mut self, tag: u64) -> Option<PendingPublish<'d>> {
        let index = self.index(tag)?;
        if self.slots[index]
            .pending
            .as_ref()
            .is_none_or(|pending| pending.tag != tag)
        {
            return None;
        }
        if self.linked(index) {
            self.unlink(index);
        }
        self.len -= 1;
        self.slots[index].pending.take()
    }

    pub(in crate::protocol) fn reserve(&mut self) -> Result<u64, Error> {
        if self.len == self.slots.len() {
            return Err(Error::CapacityOverflow("publisher confirm"));
        }
        let tag = self
            .next_tag
            .checked_add(1)
            .ok_or(Error::CapacityOverflow("publisher delivery tag"))?;
        let Some(index) = self.index(tag) else {
            return Err(Error::CapacityOverflow("publisher confirm"));
        };
        if self.slots[index].pending.is_some() {
            return Err(Error::CapacityOverflow("publisher confirm"));
        }
        self.next_tag = tag;
        self.slots[index].pending = Some(PendingPublish {
            tag,
            waker: WakeSlot::default(),
            outcome: None,
            detached: false,
        });
        self.link(index);
        self.len += 1;
        Ok(tag)
    }

    pub(in crate::protocol) fn confirm(
        &mut self,
        tag: u64,
        multiple: bool,
        outcome: PublishConfirm,
    ) -> Result<(), Error> {
        if multiple {
            while self.head != NONE {
                let index = self.head as usize;
                let Some(pending) = self.slots[index].pending.as_ref() else {
                    return Err(Error::Protocol("publisher confirm tracker is inconsistent"));
                };
                if pending.tag > tag {
                    break;
                }
                let detached = pending.detached;
                self.unlink(index);
                if detached {
                    self.slots[index].pending = None;
                    self.len -= 1;
                    continue;
                }
                let Some(pending) = self.slots[index].pending.as_mut() else {
                    return Err(Error::Protocol("publisher confirm tracker is inconsistent"));
                };
                pending.outcome = Some(Ok(outcome));
                pending.waker.wake();
            }
            return Ok(());
        }
        let Some(index) = self.index(tag) else {
            return Ok(());
        };
        if self.slots[index]
            .pending
            .as_ref()
            .is_none_or(|pending| pending.tag != tag)
        {
            return Ok(());
        }
        if self.slots[index]
            .pending
            .as_ref()
            .is_some_and(|pending| pending.outcome.is_some())
        {
            return Ok(());
        }
        self.unlink(index);
        let Some(pending) = self.slots[index].pending.as_mut() else {
            return Err(Error::Protocol("publisher confirm tracker is inconsistent"));
        };
        if pending.detached {
            self.slots[index].pending = None;
            self.len -= 1;
            return Ok(());
        }
        pending.outcome = Some(Ok(outcome));
        pending.waker.wake();
        Ok(())
    }
}
