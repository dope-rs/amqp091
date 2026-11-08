pub(crate) use next::Next;
use o3::depot::arena::linked::slots;
use o3::depot::capacity;
use o3::{alloc, depot};

use crate::service::content::delivery::Delivery;
use crate::state::channel::Freed;
use crate::state::channel::consumer::delivery::queue::Queue;
use crate::state::channel::consumer::delivery::{self};

mod next;
pub(super) mod queue;

pub(super) enum Tag {}

#[repr(transparent)]
/// Raw arena access sealed behind consumer delivery ownership.
pub(crate) struct Store<'c, 'b, 'd>(depot::raw::Arena<Delivery<'c, 'b, 'd>, delivery::Tag>);

#[repr(transparent)]
pub(crate) struct Vacant<'r, 'c, 'b, 'd>(
    slots::Vacant<Delivery<'c, 'b, 'd>, &'r slots::Store<Delivery<'c, 'b, 'd>>, delivery::Tag>,
);

#[repr(transparent)]
pub(crate) struct Claim<'r, 'c, 'b, 'd>(
    slots::Live<Delivery<'c, 'b, 'd>, &'r slots::Store<Delivery<'c, 'b, 'd>>, delivery::Tag>,
);

pub(crate) enum Route<'r, 'c, 'b, 'd> {
    Queue(Vacant<'r, 'c, 'b, 'd>),
    Discard,
}

impl<'c, 'b, 'd> Store<'c, 'b, 'd> {
    pub(crate) fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self(depot::raw::Arena::try_new(capacity.get())?))
    }

    pub(crate) fn vacant<'r>(&'r self) -> Option<Vacant<'r, 'c, 'b, 'd>> {
        self.0.vacant().map(Vacant)
    }

    fn queue<'a>(
        &'a self,
        queue: &'a mut Queue,
    ) -> slots::Bound<'a, Delivery<'c, 'b, 'd>, delivery::Tag> {
        // SAFETY: consumer storage pairs this queue with its original delivery nodes.
        unsafe { self.0.queue(&mut queue.raw) }
    }

    pub(in crate::state::channel::consumer) fn pop(
        &self,
        queue: &mut Queue,
    ) -> (Option<Delivery<'c, 'b, 'd>>, Freed) {
        let was_full = self.0.available() == 0;
        Freed::popped(was_full, self.queue(queue).pop_front())
    }

    pub(in crate::state::channel::consumer) fn drain(&self, queue: &mut Queue) -> Freed {
        let was_full = self.0.available() == 0;
        let mut queue = self.queue(queue);
        let (mut next, freed) = Freed::popped(was_full, queue.pop_front());
        while next.is_some() {
            next = queue.pop_front();
        }
        freed
    }
}

impl<'r, 'c, 'b, 'd> Vacant<'r, 'c, 'b, 'd> {
    pub(crate) fn insert(self, delivery: Delivery<'c, 'b, 'd>) -> Claim<'r, 'c, 'b, 'd> {
        Claim(self.0.insert(delivery))
    }
}

impl<'r, 'c, 'b, 'd> Claim<'r, 'c, 'b, 'd> {
    pub(in crate::state::channel::consumer) fn value(&self) -> &Delivery<'c, 'b, 'd> {
        self.0.get()
    }

    pub(in crate::state::channel::consumer) fn push(self, queue: &mut Queue) {
        slots::Live::push_back(unsafe { depot::raw::Proof::new((self.0, &mut queue.raw)) });
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Store<'static, 'static, 'static>>() == 3 * std::mem::size_of::<usize>()
    );
    assert!(
        std::mem::size_of::<Vacant<'static, 'static, 'static, 'static>>()
            == 2 * std::mem::size_of::<usize>()
    );
    assert!(
        std::mem::size_of::<Claim<'static, 'static, 'static, 'static>>()
            == 2 * std::mem::size_of::<usize>()
    );
};
