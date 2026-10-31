use dope::manifold::client::port;
use dope::net::link::egress;
use o3::buffer::pool;
use o3::depot::capacity;

use crate::failure::{self, wire};
use crate::service::cancel;
use crate::service::content::pool::Owners;
use crate::state::channel::consumer;
use crate::state::{channels, limits};
use crate::{config, permits, protocol};

pub(super) struct Transport {
    pub(super) causes: capacity::Count,
    pub(super) staging: port::Plan<()>,
}

pub(super) struct Coordination {
    pub(super) waiter_capacity: usize,
    pub(super) admissions: permits::Plan,
    pub(super) cancellations: cancel::Plan,
}

pub(super) struct EgressPlan {
    pub(super) regular: pool::Layout,
    pub(super) control: pool::Layout,
    pub(super) retained: egress::Config,
}

pub(super) struct ContentPlan {
    pub(super) owners: Owners,
    pub(super) delivery_routes: capacity::Count,
    pub(super) return_routes: capacity::Count,
    pub(super) compact: pool::Layout,
    pub(super) bytes: usize,
}

/// One validated source for both driver claims and runtime storage.
pub(super) struct Plan {
    pub(super) transport: Transport,
    pub(super) channels: channels::Plan,
    pub(super) coordination: Coordination,
    pub(super) egress: EgressPlan,
    pub(super) content: ContentPlan,
    pub(super) identities: capacity::Count,
}

impl Plan {
    pub(super) fn try_new(
        config: &config::Config,
        conns: usize,
        waiters: usize,
    ) -> Result<Self, failure::Error> {
        let (ids, count) = Self::channels(config);
        let limits = limits::Limits::try_new(config, count as usize)
            .map_err(failure::Error::InvalidConfig)?;
        let deliver = Self::entries(count, limits.deliveries, wire::Route::Delivery)?;
        let returns = Self::entries(count, limits.returns, wire::Route::Returned)?;
        let channels = channels::Plan::new(
            ids,
            count,
            limits,
            consumer::Plan::new(limits.consumers.get())?,
        );
        let cancel = cancel::Plan::try_new(count, limits)?;
        let permits = permits::Plan::try_new(waiters, count as usize)?;
        let (regular, retained, staging) = Self::outbound(config, limits)?;
        let slots = limits.control.frames;
        let cap = protocol::CONTROL_FRAME_CAPACITY;
        let control = pool::Layout::new(slots, cap).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::Pool {
                slots,
                capacity: cap,
                target: wire::Layout::Control,
                error,
            })
        })?;
        let parts = config.capacity.body.fragments;
        let parts = capacity::Count::try_from(parts).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::BodyFragments { error })
        })?;
        let owners =
            Owners::try_new(deliver.0, returns.0, parts).map_err(failure::Error::InvalidConfig)?;
        let bytes = config.capacity.body.buffer;
        let Some(slots) = deliver.0.get().checked_add(returns.0.get()) else {
            return Err(failure::Error::InvalidConfig(wire::Config::CompactSlots {
                delivery: deliver.0.get(),
                returned: returns.0.get(),
            }));
        };
        let slots = slots.min(bytes);
        let cap = match slots {
            0 => 1,
            slots => bytes / slots,
        };
        let compact = pool::Layout::new(slots, cap).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::Pool {
                slots,
                capacity: cap,
                target: wire::Layout::Compact,
                error,
            })
        })?;
        let staging = port::Plan::new(conns, staging)
            .map_err(|error| failure::Error::InvalidConfig(wire::Config::Staging(error)))?;
        let causes = config.capacity.terminal_causes;
        if causes == 0 {
            return Err(failure::Error::InvalidConfig(
                wire::Config::TerminalCausesZero,
            ));
        }
        let causes = u32::try_from(causes)
            .map(capacity::Count::new)
            .map_err(|error| {
                failure::Error::InvalidConfig(wire::Config::TerminalCauses {
                    actual: causes,
                    error,
                })
            })?;
        let ids =
            capacity::Count::try_from(config.capacity.topology.identities).map_err(|error| {
                failure::Error::InvalidConfig(wire::Config::TopologyIdentities { error })
            })?;

        Ok(Self {
            transport: Transport { causes, staging },
            channels,
            coordination: Coordination {
                waiter_capacity: waiters,
                admissions: permits,
                cancellations: cancel,
            },
            egress: EgressPlan {
                regular,
                control,
                retained,
            },
            content: ContentPlan {
                owners,
                delivery_routes: deliver.1,
                return_routes: returns.1,
                compact,
                bytes,
            },
            identities: ids,
        })
    }

    fn entries(
        channels: u32,
        queued: capacity::Count,
        route: wire::Route,
    ) -> Result<(capacity::Count, capacity::Count), failure::Error> {
        let content = channels as usize * (queued.get() + 1);
        let content = capacity::Count::try_from(content).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::ContentCount { route, error })
        })?;
        let routes = capacity::Count::new(content.raw() - channels);
        Ok((content, routes))
    }

    fn channels(config: &config::Config) -> (usize, u32) {
        let ids = if config.tune.channel_max == 0 {
            u16::MAX as usize + 1
        } else {
            config.tune.channel_max as usize + 1
        };
        (ids, config.capacity.channels.min(ids - 1) as u32)
    }

    fn outbound(
        config: &config::Config,
        limits: limits::Limits,
    ) -> Result<(pool::Layout, egress::Config, egress::Config), failure::Error> {
        let frames = config.capacity.egress.frames;
        let frames = u32::try_from(frames).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::OutboundFrames {
                actual: frames,
                error,
            })
        })?;
        if frames == 0 {
            return Err(failure::Error::InvalidConfig(wire::Config::OutboundZero));
        }
        let buffer = config.capacity.egress.buffer;
        let buffer = u32::try_from(buffer).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::OutboundBytes {
                actual: buffer,
                error,
            })
        })?;
        let cap = protocol::CONTROL_FRAME_CAPACITY;
        let cap = u32::try_from(cap).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::ControlBytes { actual: cap, error })
        })?;
        let width = buffer.max(cap);
        let resident = frames
            .checked_mul(width)
            .ok_or(failure::Error::InvalidConfig(
                wire::Config::OutboundResident {
                    frames,
                    bytes: width,
                },
            ))?;
        let layout = pool::Layout::new(frames as usize, buffer as usize).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::Pool {
                slots: frames as usize,
                capacity: buffer as usize,
                target: wire::Layout::Outbound,
                error,
            })
        })?;
        let retained = egress::Config::shared(egress::Credit::new(
            egress::Entries::new(frames),
            egress::Resident::new(resident),
        ));
        let control = limits.control.frames;
        let control = u32::try_from(control).map_err(|error| {
            failure::Error::InvalidConfig(wire::Config::ControlFrames {
                actual: control,
                error,
            })
        })?;
        let entries = frames
            .checked_add(control)
            .ok_or(failure::Error::InvalidConfig(wire::Config::SourceEntries {
                regular: frames,
                control,
            }))?;
        let source = wire::Config::SourceResident {
            regular_frames: frames,
            regular_bytes: buffer,
            control_frames: control,
            control_bytes: cap,
        };
        let regular = frames
            .checked_mul(buffer)
            .ok_or(failure::Error::InvalidConfig(source))?;
        let control = control
            .checked_mul(cap)
            .ok_or(failure::Error::InvalidConfig(source))?;
        let resident = regular
            .checked_add(control)
            .ok_or(failure::Error::InvalidConfig(source))?;
        Ok((
            layout,
            retained,
            egress::Config::shared(egress::Credit::new(
                egress::Entries::new(entries),
                egress::Resident::new(resident),
            )),
        ))
    }
}
