use std::num;

use o3::depot::capacity;

use crate::config;
use crate::failure::wire;
use crate::protocol::topology;

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub methods: Method,
    pub confirms: capacity::Count,
    pub consumers: capacity::Count,
    pub control: Control,
    pub deliveries: capacity::Count,
    pub returns: capacity::Count,
    pub resources: Resource,
}

#[derive(Clone, Copy)]
pub(crate) struct Method {
    pub user: capacity::Count,
    pub storage: num::NonZero<u32>,
}

#[derive(Clone, Copy)]
pub(crate) struct Control {
    pub frames: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct Resource {
    pub body: usize,
    pub topology: topology::Plan,
}

impl Limits {
    pub(crate) fn try_new(config: &config::Config, channels: usize) -> Result<Self, wire::Config> {
        let config::Channel {
            methods,
            confirms,
            consumers: consumer,
            deliveries: delivery,
            returns,
        } = config.capacity.channel;
        let methods = capacity::Count::try_from(methods).map_err(|error| wire::Config::Count {
            target: wire::Limit::Method,
            error,
        })?;
        let confirms =
            capacity::Count::try_from(confirms).map_err(|error| wire::Config::Count {
                target: wire::Limit::Confirm,
                error,
            })?;
        let consumer =
            capacity::Count::try_from(consumer).map_err(|error| wire::Config::Count {
                target: wire::Limit::Consumer,
                error,
            })?;
        let delivery =
            capacity::Count::try_from(delivery).map_err(|error| wire::Config::Count {
                target: wire::Limit::Delivery,
                error,
            })?;
        let returns = capacity::Count::try_from(returns).map_err(|error| wire::Config::Count {
            target: wire::Limit::Returned,
            error,
        })?;
        let Some(total) = methods.get().checked_add(consumer.get()) else {
            return Err(wire::Config::Sum {
                target: wire::Limit::MethodStore,
                left: methods.get(),
                right: consumer.get(),
            });
        };
        let Some(slots) = num::NonZero::<usize>::MIN.checked_add(total) else {
            return Err(wire::Config::Sum {
                target: wire::Limit::MethodStore,
                left: total,
                right: 1,
            });
        };
        let storage = capacity::Count::try_nonzero(slots).map_err(|error| wire::Config::Count {
            target: wire::Limit::MethodStore,
            error,
        })?;
        let Some(lanes) = consumer.get().checked_add(1) else {
            return Err(wire::Config::Sum {
                target: wire::Limit::ControlLane,
                left: consumer.get(),
                right: 1,
            });
        };
        let Some(frames) = channels.checked_mul(lanes) else {
            return Err(wire::Config::Product {
                target: wire::Limit::ControlFrame,
                left: channels,
                right: lanes,
            });
        };
        Ok(Self {
            methods: Method {
                user: methods,
                storage,
            },
            confirms,
            consumers: consumer,
            control: Control { frames },
            deliveries: delivery,
            returns,
            resources: Resource {
                body: config.capacity.body.max_size,
                topology: topology::Plan::try_new(config.capacity.topology)?,
            },
        })
    }
}
