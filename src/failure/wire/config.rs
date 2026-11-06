use std::{mem, num};

use dope::manifold::client::port::error;
use o3::buffer::pool;
use o3::depot::{capacity, map};

use crate::failure::wire::{Layout, Limit, Route};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Config {
    Count {
        target: Limit,
        error: capacity::Error,
    },
    Sum {
        target: Limit,
        left: usize,
        right: usize,
    },
    Product {
        target: Limit,
        left: usize,
        right: usize,
    },
    AdmissionLanes {
        channels: usize,
    },
    ConsumerHash {
        error: map::Capacity,
    },
    CancellationConsumer {
        methods: u32,
        consumers: u32,
    },
    CancellationConfirm {
        consumer_end: u32,
        confirms: u32,
    },
    CancellationSlots {
        confirm_end: u32,
    },
    CancellationChannelZero,
    CancellationEntryCapacity {
        channels: u32,
        slots: u32,
    },
    FrameMinimum {
        actual: u32,
        minimum: u32,
    },
    OutboundFrameOverhead,
    ContentAdd {
        route: Route,
        queued: usize,
    },
    ContentProduct {
        route: Route,
        channels: u32,
        per_channel: usize,
    },
    ContentCount {
        route: Route,
        error: capacity::Error,
    },
    RouteProduct {
        route: Route,
        channels: u32,
        queued: usize,
    },
    RouteCount {
        route: Route,
        error: capacity::Error,
    },
    Pool {
        slots: usize,
        capacity: usize,
        target: Layout,
        error: pool::LayoutError,
    },
    BodyFragments {
        error: capacity::Error,
    },
    ContentOwners {
        delivery: capacity::Count,
        returned: capacity::Count,
        fragments: capacity::Count,
        error: num::TryFromIntError,
    },
    CompactSlots {
        delivery: usize,
        returned: usize,
    },
    Staging(error::Plan),
    TerminalCausesZero,
    TerminalCauses {
        actual: usize,
        error: num::TryFromIntError,
    },
    TopologyIdentities {
        error: capacity::Error,
    },
    TopologyCount {
        target: Limit,
        error: capacity::Error,
    },
    TopologyMap {
        target: Limit,
        error: capacity::Error,
    },
    OutboundFrames {
        actual: usize,
        error: num::TryFromIntError,
    },
    OutboundZero,
    OutboundBytes {
        actual: usize,
        error: num::TryFromIntError,
    },
    ControlBytes {
        actual: usize,
        error: num::TryFromIntError,
    },
    OutboundResident {
        frames: u32,
        bytes: u32,
    },
    ControlFrames {
        actual: usize,
        error: num::TryFromIntError,
    },
    SourceEntries {
        regular: u32,
        control: u32,
    },
    SourceResident {
        regular_frames: u32,
        regular_bytes: u32,
        control_frames: u32,
        control_bytes: u32,
    },
    AuthenticationNul {
        offset: num::NonZero<usize>,
    },
}

const _: () = assert!(mem::size_of::<Config>() == 6 * mem::size_of::<usize>());
