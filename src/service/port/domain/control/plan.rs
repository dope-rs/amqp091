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
        match method.preview(channel, &next, |command| {
            egress.control().map(|frame| frame.write(channel, command))
        }) {
            Ok(Some(Some(Ok(frame)))) => Self::Send(frame, next, freed),
            Ok(Some(Some(Err(cause)))) => Self::Failed(cause),
            Ok(None | Some(None)) => Self::Pending(Some(next), freed),
            Err(cause) => Self::Failed(failure::Error::Protocol(cause)),
        }
    }
}

const _: () = {
    const SEND: usize = mem::size_of::<(pool::Cursor, send::Next, channel::Freed)>();
    const FAILED: usize = mem::size_of::<failure::Error>();
    const PAYLOAD: usize = if SEND > FAILED { SEND } else { FAILED };
    assert!(mem::size_of::<Plan<send::Next>>() <= PAYLOAD + mem::size_of::<usize>());
};
