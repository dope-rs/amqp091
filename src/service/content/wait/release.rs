use std::mem;

use crate::service::content::payload;
use crate::service::content::wait::resource;

pub(in crate::service::content) struct Release {
    pub(super) entry: Option<resource::Entry>,
    pub(super) body: usize,
    pub(super) resident: bool,
    pub(super) complete: bool,
    pub(super) fragments: u32,
}

const _: () = assert!(mem::size_of::<Release>() == 2 * mem::size_of::<usize>());

impl Release {
    pub(in crate::service::content) fn entry(
        entry: resource::Entry,
        property: bool,
        payload: Option<payload::Release>,
    ) -> Self {
        let (body, resident, complete, frags) = match payload {
            Some(payload) => (
                payload.bytes,
                payload.resident,
                payload.complete,
                payload.fragments,
            ),
            None => (0, false, false, 0),
        };
        Self {
            entry: Some(entry),
            body,
            resident: resident || property,
            complete,
            fragments: frags,
        }
    }

    pub(in crate::service::content) const fn payload(payload: payload::Release) -> Self {
        Self {
            entry: None,
            body: payload.bytes,
            resident: payload.resident,
            complete: payload.complete,
            fragments: payload.fragments,
        }
    }
}
