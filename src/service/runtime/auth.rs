use data::compose;
use dope::net::link::egress::data;
use o3::buffer::pool;

use crate::state::channels;

#[derive(Clone, Copy)]
pub(crate) enum Authority<'d, const ID: u8> {
    Connection,
    Channel(channels::Lease<'d, ID>),
}

impl<'d, const ID: u8> Authority<'d, ID> {
    const fn new(lease: Option<channels::Lease<'d, ID>>) -> Self {
        match lease {
            Some(lease) => Self::Channel(lease),
            None => Self::Connection,
        }
    }

    pub(crate) const fn tagged(
        frame: pool::Cursor,
        lease: Option<channels::Lease<'d, ID>>,
    ) -> compose::Tagged<pool::Cursor, Self> {
        compose::Tagged::new(frame, Self::new(lease))
    }
}

const _: () = assert!(
    std::mem::size_of::<compose::Tagged<pool::Cursor, Authority<'static, 0>>>()
        == std::mem::size_of::<pool::Cursor>() + std::mem::size_of::<usize>()
);
