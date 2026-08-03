use super::super::*;

pub struct AmqpPort<'d> {
    pub(crate) io: Port<'d, Outbound<'d>>,
    shared: UnsafeCell<Shared<'d>>,
    timer: &'d Timer<'d>,
    config: Config,
    outbound: Pool,
    egress: dope_net::link::egress::storage::Storage,
    _bodies: Box<BodyPool>,
}

impl<'d> AmqpPort<'d> {
    pub fn new(
        config: Config,
        capacity: usize,
        waiter_capacity: usize,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Result<Self, Error> {
        let outbound_layout = Self::outbound_layout(&config)?;
        Ok(Self::with_layout(
            config,
            outbound_layout,
            capacity,
            waiter_capacity,
            driver,
        ))
    }

    pub(in crate::protocol) fn outbound_layout(config: &Config) -> Result<PoolLayout, Error> {
        let slots = u32::try_from(config.outbound_capacity)
            .map_err(|_| Error::InvalidConfig("outbound capacity exceeds u32"))?;
        let capacity = u32::try_from(config.outbound_buffer_capacity)
            .map_err(|_| Error::InvalidConfig("outbound buffer capacity exceeds u32"))?;
        PoolLayout::new(slots, capacity)
            .map_err(|_| Error::InvalidConfig("outbound pool layout is invalid"))
    }

    pub(in crate::protocol) fn with_layout(
        config: Config,
        outbound_layout: PoolLayout,
        capacity: usize,
        waiter_capacity: usize,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Self {
        let mut bodies = Box::new(BodyPool::with_capacity(config.body_fragment_capacity));
        let shared = Shared::new(&config, waiter_capacity, NonNull::from(bodies.as_mut()));
        let driver_ref = driver.driver_ref();
        Self {
            io: Port::with_capacity(capacity, driver.region_token_ref(), driver_ref),
            shared: UnsafeCell::new(shared),
            timer: driver.timer(),
            outbound: Pool::from_layout(outbound_layout),
            egress: dope_net::link::egress::storage::Storage::default(),
            config,
            _bodies: bodies,
        }
    }

    pub fn capacity(&self) -> usize {
        self.io.capacity()
    }

    pub(crate) fn encode(
        &'d self,
        f: impl FnOnce(&mut Outbound<'d>),
    ) -> Result<Outbound<'d>, Error> {
        let mut outbound = self
            .outbound
            .try_acquire()
            .map(Outbound::new)
            .ok_or(Error::CapacityOverflow("outbound buffer"))?;
        f(&mut outbound);
        if outbound.overflowed() {
            return Err(Error::CapacityOverflow("outbound buffer"));
        }
        Ok(outbound)
    }

    pub(crate) fn with_shared<R>(&self, f: impl for<'s> FnOnce(&'s mut Shared<'d>) -> R) -> R {
        f(unsafe { &mut *self.shared.get() })
    }

    pub fn timer(&'d self) -> &'d Timer<'d> {
        self.timer
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn egress(&self) -> &dope_net::link::egress::storage::Storage {
        &self.egress
    }

    pub fn factory(
        config: Config,
        capacity: usize,
        waiter_capacity: usize,
    ) -> Result<AmqpPortFactory, Error> {
        let outbound_layout = Self::outbound_layout(&config)?;
        Ok(AmqpPortFactory {
            config,
            outbound_layout,
            capacity,
            waiter_capacity,
        })
    }
}
