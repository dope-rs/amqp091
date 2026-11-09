use std::marker;

use crate::state::channel::{consumer, methods};

#[derive(Clone, Copy)]
pub(crate) enum Reservation {}
#[derive(Clone, Copy)]
pub enum Method {}
#[derive(Clone, Copy)]
pub(crate) enum Consumer {}
#[derive(Clone, Copy)]
pub(crate) enum Confirm {}
#[derive(Clone, Copy)]
pub(crate) enum Publish {}
#[derive(Clone, Copy)]
pub(crate) enum Control {}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Reservation = 0,
    Method = 1,
    Consumer = 2,
    Confirm = 3,
    Publish = 4,
    Control = 5,
}

impl Kind {
    pub(crate) const COUNT: usize = 6;

    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Slot<L> {
    raw: u32,
    lane: marker::PhantomData<L>,
}

impl<L> Slot<L> {
    pub(in crate::state::channel) const fn new(raw: u32) -> Self {
        Self {
            raw,
            lane: marker::PhantomData,
        }
    }

    pub(crate) const fn raw(self) -> u32 {
        self.raw
    }
}

impl Slot<Reservation> {
    pub(crate) const ZERO: Self = Self::new(0);
}

impl Slot<Publish> {
    pub(crate) const ZERO: Self = Self::new(0);
}

impl Slot<Control> {
    pub(crate) const ZERO: Self = Self::new(0);
}

impl From<methods::MethodKey> for Slot<Method> {
    fn from(key: methods::MethodKey) -> Self {
        Self::new(key.index())
    }
}

impl From<consumer::Key> for Slot<Consumer> {
    fn from(key: consumer::Key) -> Self {
        Self::new(key.index())
    }
}

const _: () = assert!(size_of::<Slot<Method>>() == size_of::<u32>());
