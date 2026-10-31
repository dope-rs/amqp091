use std::num::NonZero;

use crate::state::channel::consumer;

#[derive(Debug)]
pub struct Clash {
    pub incoming: NonZero<u64>,
    pub stored: NonZero<u64>,
}

impl Clash {
    pub(crate) fn new(incoming: consumer::Key, stored: consumer::Key) -> Self {
        Self {
            incoming: incoming.into(),
            stored: stored.into(),
        }
    }
}

/// Brand for one outstanding method slot and its exact lookup failures.

#[derive(Debug)]
pub enum Request {}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Index {
    Staged,
    Indexed,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Pending,
    Replay,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consumer {
    Pending,
    Active,
    Untracked,
    Recovering,
    Replaying,
    Canceled,
    Retiring,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Missing,
    Pending,
    Waiting,
    Control,
    Sent,
    Ready,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Control {
    None,
    Cancel,
    Consumer,
    Orphan,
    Close,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flight {
    Empty,
    Work,
    Identity,
    IdentityReuse,
    ConsumerNew,
    ConsumerReplay,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Use {
    Vacant,
    Complete,
    Queue,
    Consumer,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Identity {
    Missing,
    User,
    Vacant,
    Server,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    Empty,
    Queue,
    Purge,
    Delete,
    Consume,
    Replay,
    Cancel,
}
