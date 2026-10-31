//! Driver-branded semantic capabilities for external runtime adapters.

use std::mem;

use crate::service;

mod core;
mod poll;

pub use poll::Poll;
mod step;

pub mod channel;
pub mod conn;
pub mod consumer;
pub mod method;
pub mod publish;

/// One-pointer entry capability for the adapter surface.
///
/// Pre-encoded frames are not exposed, and the driver lifetime cannot be widened.
#[repr(transparent)]
pub struct Adapter<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID>,
}

impl<const ID: u8> Copy for Adapter<'_, '_, '_, '_, '_, ID> {}

impl<const ID: u8> Clone for Adapter<'_, '_, '_, '_, '_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Adapter<'o, 'r, 'c, 'b, 'd, ID> {
    pub const fn new(port: &'o service::Port<'r, 'c, 'b, 'd, ID>) -> Self {
        Self {
            core: core::Core::new(port),
        }
    }

    pub const fn connection(self) -> conn::Connection<'o, 'r, 'c, 'b, 'd, ID> {
        conn::Connection::new(self.core.project())
    }

    pub const fn channels(self) -> channel::Channels<'o, 'r, 'c, 'b, 'd, ID> {
        channel::Channels::new(self.core.project())
    }

    pub const fn methods(self) -> method::Methods<'o, 'r, 'c, 'b, 'd, ID> {
        method::Methods::new(self.core.project())
    }

    pub const fn consumers(self) -> consumer::Consumers<'o, 'r, 'c, 'b, 'd, ID> {
        consumer::Consumers::new(self.core.project())
    }

    pub const fn publish(self) -> publish::Publish<'o, 'r, 'c, 'b, 'd, ID> {
        publish::Publish::new(self.core.project())
    }
}

const _: () = assert!(
    mem::size_of::<Adapter<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<usize>()
);
const _: () = assert!(
    mem::align_of::<Adapter<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::align_of::<usize>()
);
