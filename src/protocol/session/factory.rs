use super::super::*;

pub struct AmqpPortFactory {
    pub(super) config: Config,
    pub(super) outbound_layout: PoolLayout,
    pub(super) capacity: usize,
    pub(super) waiter_capacity: usize,
}

impl StorageFactory for AmqpPortFactory {
    type Output<'d> = AmqpPort<'d>;

    fn build<'d>(self, driver: &mut dope::DriverContext<'_, 'd>) -> Self::Output<'d> {
        AmqpPort::with_layout(
            self.config,
            self.outbound_layout,
            self.capacity,
            self.waiter_capacity,
            driver,
        )
    }
}
