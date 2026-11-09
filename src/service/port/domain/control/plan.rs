use std::mem;

use o3::buffer::pool;

use crate::failure;
use crate::service::port::domain::egress;
use crate::state::channel::methods::send;
use crate::state::channel::{self, facets};

#[must_use = "write plan owns any route freed while preparing it"]
pub(super) enum Plan<P> {
    Send(pool::Cursor, P, channel::Freed),
    Pending(Option<P>, channel::Freed),
    Missing(channel::Freed),
    Failed(failure::Error),
}

impl Plan<send::Next> {
    pub(super) fn encode<const ID: u8>(
        method: &mut facets::Method<'_, '_, '_, '_, ID>,
        egress: Option<&egress::Egress>,
        channel: u16,
        next: send::Next,
        freed: channel::Freed,
    ) -> Self {
        let Some(egress) = egress else {
            return Self::Pending(Some(next), freed);
        };
        let command = match method.preview(channel, &next) {
            Ok(Some(command)) => command,
            Ok(None) => return Self::Pending(Some(next), freed),
            Err(cause) => return Self::Failed(failure::Error::Protocol(cause)),
        };
        let Some(frame) = egress.control() else {
            return Self::Pending(Some(next), freed);
        };
        match frame.write(channel, command) {
            Ok(frame) => Self::Send(frame, next, freed),
            Err(cause) => Self::Failed(cause),
        }
    }
}

const _: () = {
    const SEND: usize = mem::size_of::<(pool::Cursor, send::Next, channel::Freed)>();
    const FAILED: usize = mem::size_of::<failure::Error>();
    const PAYLOAD: usize = if SEND > FAILED { SEND } else { FAILED };
    assert!(mem::size_of::<Plan<send::Next>>() <= PAYLOAD + mem::size_of::<usize>());
};
