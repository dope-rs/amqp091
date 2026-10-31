use std::num;

use o3::base::cell::{local, region};

use crate::adapter::channel::route;
use crate::adapter::core;
use crate::failure::{self, wire};
use crate::protocol::wire::frame;
use crate::service;

#[derive(Clone, Copy)]
pub(super) struct Limit(num::NonZero<u32>);

impl Limit {
    pub(super) fn read<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        access: local::Read<'_, region::Identity<'d>>,
    ) -> Result<Self, failure::Runtime<'c, 'b, 'd, ID>> {
        let tune = match route.read(access, |state| state.connection.negotiated()) {
            service::EpochAccess::Live(tune) => tune,
            service::EpochAccess::Terminal(cause) => {
                return Err(cause.branded());
            }
            service::EpochAccess::Unknown => {
                return Err(failure::Runtime::from(failure::Error::Closed));
            }
        };
        let buffer = route.core().config().capacity.egress.buffer;
        let protocol = if tune.frame_max == 0 {
            buffer
        } else {
            tune.frame_max as usize
        };
        let body = protocol
            .min(buffer)
            .checked_sub(frame::OVERHEAD)
            .and_then(|value| num::NonZero::<u32>::new(value as u32))
            .ok_or(failure::Runtime::from(failure::Error::InvalidConfig(
                wire::Config::OutboundFrameOverhead,
            )))?;
        Ok(Self(body))
    }

    pub(super) const fn frame(self) -> usize {
        self.0.get() as usize + frame::OVERHEAD
    }

    pub(super) const fn body(self) -> num::NonZero<u32> {
        self.0
    }
}

const _: () = assert!(std::mem::size_of::<Limit>() == std::mem::size_of::<u32>());
