use std::{mem, ptr};

use crate::protocol::wire::text::short;

mod bind;
mod cursor;
mod declare;
mod delete;
mod key;
mod token;

pub(crate) use bind::Bind;
pub(crate) use cursor::Cursor;
pub(crate) use declare::Declare;
pub(crate) use delete::Delete;
pub(crate) use key::Key;
pub(crate) use token::Token;

pub(crate) enum Tag {}

#[repr(u8)]
pub(crate) enum Current<'d, const ID: u8> {
    None,
    Tracked(Key<'d, ID>),
    Named { name: short::InlineName, live: bool },
}

#[repr(C)]
struct Named {
    tag: u8,
    name: short::InlineName,
    live: bool,
}

const NAMED_TAG: u8 = {
    let value = Current::<'static, 0>::Named {
        name: short::InlineName::EMPTY,
        live: true,
    };
    // SAFETY: repr(u8) stores the initialized discriminant in the first byte.
    unsafe { ptr::from_ref(&value).cast::<u8>().read() }
};

impl<'d, const ID: u8> Current<'d, ID> {
    pub(in crate::protocol::topology) fn set_name(&mut self, value: short::Name<'_>) {
        let current = ptr::from_mut(self);
        let target = current.cast::<Named>();
        // SAFETY: repr(u8) gives Named this repr(C) tag/payload layout. The
        // exclusive owner drops the old variant before initializing the new
        // prefix, length and live flag, then publishes its discriminant.
        unsafe {
            ptr::drop_in_place(current);
            short::InlineName::initialize(ptr::addr_of_mut!((*target).name), value);
            ptr::addr_of_mut!((*target).live).write(true);
            ptr::addr_of_mut!((*target).tag).write(NAMED_TAG);
        }
    }

    pub(in crate::protocol::topology) const fn key(&self) -> Option<Key<'d, ID>> {
        match self {
            Self::Tracked(key) => Some(*key),
            Self::None | Self::Named { .. } => None,
        }
    }

    pub(in crate::protocol::topology) fn name(&self) -> Option<short::Name<'_>> {
        match self {
            Self::Named { name, .. } => Some(name.name()),
            Self::None | Self::Tracked(_) => None,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Named>() <= mem::size_of::<Current<'static, 0>>());
    assert!(mem::align_of::<Named>() <= mem::align_of::<Current<'static, 0>>());
};

/// None follows the queue name across recovery; Some is a literal routing key.
pub(in crate::protocol::topology) fn routing_key<'a>(
    name: short::Short<'a>,
    key: short::Short<'a>,
) -> Option<short::Short<'a>> {
    (!(name.is_empty() && key.is_empty())).then_some(key)
}
