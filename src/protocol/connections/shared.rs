use super::super::*;

pub(crate) struct Shared<'d> {
    pub(crate) ready_conn: Option<Token>,
    pub(crate) handshake_wakers: WakeRegistry<'d>,
    pub(crate) fatal: Option<Error>,
    pub(crate) closed_wakers: WakeRegistry<'d>,
    pub(crate) progress_wakers: WakeRegistry<'d>,
    pub(crate) last_recv_at: Option<Instant>,
    pub(crate) topology: Topology,
    pub(crate) auto_recovery_enabled: bool,
    pub(crate) intentionally_closed: bool,
    pub(crate) ready_generation: u64,
    pub(crate) blocked: bool,
    pub(crate) blocked_reason: String,
    pub(in crate::protocol) free_channels: FreeChannels,
    pub negotiated: Negotiated,
    pub(in crate::protocol) channels: Channels<'d>,
    _budget: Pin<Box<ByteBudget>>,
}

impl<'d> Shared<'d> {
    pub(in crate::protocol) fn new(
        config: &Config,
        waiter_capacity: usize,
        body_pool: NonNull<BodyPool>,
    ) -> Self {
        let budget = Box::pin(ByteBudget::new(config.buffer_capacity));
        // SAFETY: `_budget` is pinned inside `Shared` and every handle is
        // discarded before that owner is dropped.
        let handle = unsafe {
            transmute::<ByteBudgetHandle<'_>, ByteBudgetHandle<'static>>(budget.as_ref().handle())
        };
        let id_capacity = if config.channel_max == 0 {
            u16::MAX as usize + 1
        } else {
            config.channel_max as usize + 1
        };
        Self {
            ready_conn: None,
            handshake_wakers: WakeRegistry::with_capacity(waiter_capacity),
            fatal: None,
            closed_wakers: WakeRegistry::with_capacity(waiter_capacity),
            progress_wakers: WakeRegistry::with_capacity(waiter_capacity),
            last_recv_at: None,
            topology: Topology::default(),
            auto_recovery_enabled: false,
            intentionally_closed: false,
            ready_generation: 0,
            blocked: false,
            blocked_reason: String::new(),
            free_channels: FreeChannels::new(id_capacity),
            negotiated: Negotiated::default(),
            channels: Channels::new(config, handle, body_pool),
            _budget: budget,
        }
    }

    pub(crate) fn with_channel_mut<R>(
        &self,
        id: u16,
        f: impl for<'a> FnOnce(&'a mut ChannelState<'d>) -> R,
    ) -> Option<R> {
        self.channels.with_channel_mut(id, f)
    }

    pub(crate) fn is_blocked(&self) -> bool {
        self.blocked
    }

    pub(crate) fn blocked_reason(&self) -> &str {
        &self.blocked_reason
    }

    pub(crate) fn try_register_handshake_waker(&mut self, waker: Waker<'d>) -> bool {
        self.handshake_wakers.register(waker)
    }

    pub(crate) fn try_register_closed_waker(&mut self, waker: Waker<'d>) -> bool {
        self.closed_wakers.register(waker)
    }

    pub(crate) fn try_register_progress_waker(&mut self, waker: Waker<'d>) -> bool {
        self.progress_wakers.register(waker)
    }

    pub(crate) fn try_register_recovery_waker(&mut self, waker: Waker<'d>) -> bool {
        if !self.progress_wakers.can_register(waker) || !self.handshake_wakers.can_register(waker) {
            return false;
        }
        self.progress_wakers.register(waker) && self.handshake_wakers.register(waker)
    }

    pub(crate) fn ready_generation(&self) -> u64 {
        self.ready_generation
    }

    pub(crate) fn auto_recovery_enabled(&self) -> bool {
        self.auto_recovery_enabled
    }

    pub(crate) fn intentionally_closed(&self) -> bool {
        self.intentionally_closed
    }

    pub(crate) fn enable_auto_recovery(&mut self) {
        self.auto_recovery_enabled = true;
    }

    pub(crate) fn mark_intentional_close(&mut self) {
        self.intentionally_closed = true;
    }

    pub(crate) fn topology_mut(&mut self) -> &mut Topology {
        &mut self.topology
    }

    pub(crate) fn topology(&self) -> &Topology {
        &self.topology
    }

    pub(crate) fn next_unused_channel(&mut self, channel_max: u16) -> Option<u16> {
        let upper = if channel_max == 0 {
            u16::MAX
        } else {
            channel_max
        };
        let id = self.free_channels.take_first(upper)?;
        if self.channels.acquire(id) {
            Some(id)
        } else {
            self.free_channels.put(id);
            None
        }
    }

    pub(crate) fn release_channel(&mut self, id: u16) {
        self.channels.release(id);
        self.free_channels.put(id);
        self.topology.remove_channel(id);
    }

    pub(crate) fn collect_channel(&mut self, id: u16) {
        if self
            .with_channel_mut(id, |channel| channel.can_release())
            .unwrap_or(false)
        {
            self.release_channel(id);
        }
    }

    pub(crate) fn reserve_channel(&mut self, id: u16) -> Result<(), Error> {
        if id == 0 {
            return Err(Error::Protocol("channel 0 is reserved"));
        }
        let channel_max = self.negotiated.channel_max;
        if channel_max != 0 && id > channel_max {
            return Err(Error::Protocol("channel exceeds negotiated maximum"));
        }
        if !self.free_channels.take(id) {
            return Err(Error::ChannelInUse(id));
        }
        if !self.channels.acquire(id) {
            self.free_channels.put(id);
            return Err(Error::CapacityOverflow("channel"));
        }
        Ok(())
    }

    pub(crate) fn record_failure(&mut self, err: Error) {
        if self.fatal.is_none() {
            self.fatal = Some(err);
        }
        self.ready_conn = None;
        self.handshake_wakers.wake();
        self.closed_wakers.wake();
    }

    pub(crate) fn record_recoverable_disconnect(&mut self) {
        self.ready_conn = None;
        self.handshake_wakers.wake();
        self.closed_wakers.wake();
        self.progress_wakers.wake();
    }

    pub(crate) fn clear_dead_ready_conn(&mut self) -> bool {
        if self.ready_conn.take().is_some() {
            self.handshake_wakers.wake();
            self.closed_wakers.wake();
            self.progress_wakers.wake();
            true
        } else {
            false
        }
    }
}
