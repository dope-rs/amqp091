use o3::depot::{capacity, map};

use crate::config;
use crate::failure::wire;

#[derive(Clone, Copy)]
pub(super) struct Set {
    pub(super) slots: capacity::Count,
    pub(super) index: map::Plan,
}

#[derive(Clone, Copy)]
pub(crate) struct Plan {
    pub(super) exchange: Set,
    pub(super) queue: Set,
    pub(super) queue_bind: Set,
    pub(super) exchange_bind: Set,
}

impl Set {
    fn try_new(count: usize, target: wire::Limit) -> Result<Self, wire::Config> {
        let slots = capacity::Count::try_from(count)
            .map_err(|error| wire::Config::TopologyCount { target, error })?;
        let index =
            map::Plan::new(count).map_err(|error| wire::Config::TopologyMap { target, error })?;
        Ok(Self { slots, index })
    }
}

impl Plan {
    pub(crate) const fn queues(self) -> capacity::Count {
        self.queue.slots
    }

    #[inline(always)]
    pub(crate) fn try_new(cap: config::Topology) -> Result<Self, wire::Config> {
        Ok(Self {
            exchange: Set::try_new(cap.exchange_cap(), wire::Limit::Exchange)?,
            queue: Set::try_new(cap.queue_cap(), wire::Limit::Queue)?,
            queue_bind: Set::try_new(cap.route_cap(), wire::Limit::QueueBind)?,
            exchange_bind: Set::try_new(cap.link_cap(), wire::Limit::ExchangeBind)?,
        })
    }
}
