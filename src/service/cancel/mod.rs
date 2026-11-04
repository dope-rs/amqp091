use std::mem;

use crate::state::channel::cancel::lanes::{Confirm, Consumer, Method, Publish, Reservation, Slot};

mod lane;
mod layout;
pub(crate) mod pending;
mod plan;
mod registry;
mod signal;
mod ticket;

pub(crate) use lane::Lane;
use pending::Pending;
pub(crate) use plan::Plan;
pub(crate) use registry::Registry;
pub(crate) use signal::Signal;
pub(crate) use ticket::Ticket;

const fn assert_lane_layout<L>()
where
    L: for<'d> Lane<'d, 0>,
{
    assert!(mem::size_of::<Ticket<'_, '_, L, 0>>() == 4 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<Ticket<'_, '_, L, 0>>>() == mem::size_of::<Ticket<'_, '_, L, 0>>()
    );
    assert!(mem::size_of::<Slot<L>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Pending<'_, L, 0>>() == 2 * mem::size_of::<usize>());
}

const _: () = {
    assert_lane_layout::<Reservation>();
    assert_lane_layout::<Method>();
    assert_lane_layout::<Consumer>();
    assert_lane_layout::<Confirm>();
    assert_lane_layout::<Publish>();
};
const _: () = assert!(mem::size_of::<Registry<'static, 0>>() <= 15 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Signal<'static, 'static, 0>>() <= 5 * mem::size_of::<usize>());
