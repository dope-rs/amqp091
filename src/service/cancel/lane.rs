use std::num;

use publish::{confirms, streams};

use crate::state::channel::cancel::lanes;
use crate::state::channel::{consumer, methods, publish};
use crate::state::channels;

pub trait Lane<'d, const ID: u8>: Copy {
    type Identity: Copy + Into<num::NonZero<u64>>;

    const KIND: lanes::Kind;
}

impl<'d, const ID: u8> Lane<'d, ID> for lanes::Reservation {
    type Identity = channels::Lease<'d, ID>;

    const KIND: lanes::Kind = lanes::Kind::Reservation;
}

impl<const ID: u8> Lane<'_, ID> for lanes::Method {
    type Identity = methods::MethodKey;

    const KIND: lanes::Kind = lanes::Kind::Method;
}

impl<const ID: u8> Lane<'_, ID> for lanes::Consumer {
    type Identity = consumer::Key;

    const KIND: lanes::Kind = lanes::Kind::Consumer;
}

impl<'d, const ID: u8> Lane<'d, ID> for lanes::Confirm {
    type Identity = confirms::Key<'d>;

    const KIND: lanes::Kind = lanes::Kind::Confirm;
}

impl<const ID: u8> Lane<'_, ID> for lanes::Publish {
    type Identity = streams::Key;

    const KIND: lanes::Kind = lanes::Kind::Publish;
}
