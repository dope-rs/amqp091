use std::num;

use crate::service::cancel::registry::Retry;
use crate::state::channel::cancel::lanes;
use crate::state::channels;

pub(crate) enum Signal<'a, 'd, const ID: u8> {
    Cancel {
        lease: channels::Lease<'d, ID>,
        kind: lanes::Kind,
        identity: num::NonZero<u64>,
    },
    Control(Retry<'a, 'd, ID>),
}
