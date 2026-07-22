use super::super::*;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::protocol) enum ChannelPhase {
    #[default]
    Closed,
    Opening,
    Open,
    Closing,
}

#[derive(Debug, Default)]
pub(in crate::protocol) enum InboundContent {
    #[default]
    Idle,
    AwaitingHeader(PendingMethod),
    AwaitingBody {
        method: PendingMethod,
        header: ContentHeader,
        body: Body<'static>,
    },
}

#[derive(Debug)]
pub(in crate::protocol) enum PendingMethod {
    Deliver(basic::Deliver),
    Return(basic::Return),
}

pub(crate) struct ChannelState<'d> {
    pub(in crate::protocol) limits: Limits,
    pub(in crate::protocol) phase: ChannelPhase,
    pub(in crate::protocol) inbound: InboundContent,
    pub(in crate::protocol) pending_ops: Slab<PendingMethodOp<'d>>,
    pub(in crate::protocol) op_order: SlotQueue<SlabKey>,
    pub(in crate::protocol) consumers: ConsumerTable<'d>,
    pub(in crate::protocol) deliveries: Slab<DeliveryNode>,
    pub(in crate::protocol) confirm: ConfirmTracker<'d>,
    pub(in crate::protocol) confirm_enabled: bool,
    pub(in crate::protocol) flow_active: bool,
    pub(in crate::protocol) returns: FixedQueue<Returned<'static>>,
    pub(in crate::protocol) return_waker: WakeSlot<'d>,
    pub(in crate::protocol) budget: ByteBudgetHandle<'static>,
    pub(in crate::protocol) body_pool: NonNull<BodyPool>,
}

impl<'d> ChannelState<'d> {
    pub(in crate::protocol) fn clear_inbound(&mut self) {
        drop(take(&mut self.inbound));
    }

    pub(in crate::protocol) fn clear_messages(&mut self) {
        self.clear_inbound();
        for consumer in self.consumers.values_mut() {
            while let Some(key) = consumer.head {
                let Some(delivery) = self.deliveries.remove(key) else {
                    consumer.head = None;
                    break;
                };
                consumer.head = delivery.next;
            }
            consumer.tail = None;
        }
        self.returns.clear();
    }

    pub(in crate::protocol) fn begin_content(
        &mut self,
        method: PendingMethod,
    ) -> Result<(), Error> {
        if self.phase != ChannelPhase::Open {
            return Err(Error::Protocol("content method on closed channel"));
        }
        if !matches!(self.inbound, InboundContent::Idle) {
            return Err(Error::Protocol("content method while body is pending"));
        }
        self.inbound = InboundContent::AwaitingHeader(method);
        Ok(())
    }

    pub(in crate::protocol) fn body(&self) -> Body<'static> {
        Body::new(unsafe { self.body_pool.as_ref() })
    }

    pub(in crate::protocol) fn new(
        limits: Limits,
        budget: ByteBudgetHandle<'static>,
        body_pool: NonNull<BodyPool>,
    ) -> Self {
        Self {
            limits,
            phase: ChannelPhase::default(),
            inbound: InboundContent::default(),
            pending_ops: Slab::with_capacity(limits.methods),
            op_order: SlotQueue::with_capacity(limits.methods),
            consumers: ConsumerTable::with_capacity(limits.consumers),
            deliveries: Slab::with_capacity(limits.deliveries),
            confirm: ConfirmTracker::new(limits.confirms),
            confirm_enabled: false,
            flow_active: true,
            returns: FixedQueue::with_capacity(limits.returns),
            return_waker: WakeSlot::default(),
            budget,
            body_pool,
        }
    }
}

impl<'d> ChannelState<'d> {
    pub(crate) fn flow_active(&self) -> bool {
        self.flow_active
    }

    pub(crate) fn set_flow_active(&mut self, active: bool) {
        self.flow_active = active;
    }

    pub(crate) fn enqueue_op(
        &mut self,
        expect: ExpectMethod,
        frame: Outbound,
    ) -> Result<(SlabKey, Option<Outbound>), Error> {
        let next_phase = match (self.phase, expect) {
            (ChannelPhase::Closed, ExpectMethod::ChannelOpenOk) => Some(ChannelPhase::Opening),
            (ChannelPhase::Open, ExpectMethod::ChannelCloseOk) => Some(ChannelPhase::Closing),
            (ChannelPhase::Open, _) => None,
            _ => return Err(Error::Closed),
        };
        if self.pending_ops.len() >= self.limits.methods {
            return Err(Error::CapacityOverflow("method"));
        }
        let send = self.op_order.is_empty();
        let (frame, send_frame) = if send {
            (None, Some(frame))
        } else {
            (Some(frame), None)
        };
        let key = self
            .pending_ops
            .insert(PendingMethodOp {
                expect,
                frame,
                waker: WakeSlot::default(),
                detached: false,
                sent: send,
                outcome: None,
            })
            .map_err(|_| Error::CapacityOverflow("method"))?;
        self.op_order
            .push_back(key.index() as usize, key)
            .map_err(|_| Error::CapacityOverflow("method order"))?;
        if let Some(phase) = next_phase {
            self.phase = phase;
        }
        Ok((key, send_frame))
    }

    pub(crate) fn poll_op(
        &mut self,
        key: SlabKey,
        waker: Waker<'d>,
    ) -> Option<Result<PendingResponse, Error>> {
        let op = self.pending_ops.get_mut(key)?;
        if let Some(outcome) = op.outcome.take() {
            self.pending_ops.remove(key);
            return Some(outcome);
        }
        if op.waker.register(waker) {
            None
        } else {
            Some(Err(Error::WaiterCapacity))
        }
    }

    pub(crate) fn cancel_op(&mut self, key: SlabKey) {
        if let Some(op) = self.pending_ops.remove(key) {
            match op.expect {
                ExpectMethod::ChannelOpenOk => self.phase = ChannelPhase::Closed,
                ExpectMethod::ChannelCloseOk => self.phase = ChannelPhase::Open,
                _ => {}
            }
        }
        self.op_order.remove(key.index() as usize);
    }

    pub(crate) fn detach_op(&mut self, key: SlabKey) {
        let Some(op) = self.pending_ops.get_mut(key) else {
            return;
        };
        if op.outcome.is_some() {
            self.pending_ops.remove(key);
            return;
        }
        if op.expect == ExpectMethod::ChannelCloseOk {
            op.detached = true;
            op.waker.clear();
        } else if op.sent {
            op.detached = true;
        } else {
            self.pending_ops.remove(key);
            self.op_order.remove(key.index() as usize);
        }
    }

    pub(crate) fn register_consumer(&mut self, tag: String) -> Result<SlabKey, Error> {
        if let Some(key) = self.consumers.key(&tag) {
            while self.pop_delivery(key).is_some() {}
            let Some(consumer) = self.consumers.get_mut(key) else {
                return Err(Error::Protocol("consumer index is inconsistent"));
            };
            consumer.canceled = false;
            return Ok(key);
        }
        self.consumers.insert(ConsumerSlot {
            tag,
            head: None,
            tail: None,
            waker: WakeSlot::default(),
            canceled: false,
        })
    }

    pub(crate) fn reset_for_reopen(&mut self) {
        self.clear_messages();
        for consumer in self.consumers.values_mut() {
            consumer.canceled = true;
        }
        self.confirm.clear();
        self.confirm_enabled = false;
        self.phase = ChannelPhase::Open;
        self.flow_active = true;
        self.returns.clear();
    }

    pub(in crate::protocol) fn prepare_reuse(&mut self) {
        self.clear_messages();
        self.pending_ops.clear();
        self.op_order.clear();
        self.consumers.clear();
        self.deliveries.clear();
        self.confirm.clear();
        self.confirm_enabled = false;
        self.phase = ChannelPhase::Closed;
        self.flow_active = true;
        self.return_waker.clear();
    }

    pub(in crate::protocol) fn retire(&mut self) {
        self.phase = ChannelPhase::Closed;
        self.clear_messages();
    }

    pub(crate) fn is_retiring(&self) -> bool {
        self.phase != ChannelPhase::Open
    }

    pub(in crate::protocol) fn can_release(&self) -> bool {
        self.phase == ChannelPhase::Closed
            && self.pending_ops.is_empty()
            && self.consumers.is_empty()
            && self.confirm.is_empty()
    }

    pub(crate) fn enable_confirm(&mut self) {
        self.confirm_enabled = true;
    }

    pub(crate) fn confirm_enabled(&self) -> bool {
        self.confirm_enabled
    }

    pub(crate) fn reserve_publish(&mut self) -> Result<u64, Error> {
        if !self.confirm_enabled {
            return Err(Error::Protocol("publisher confirms not enabled"));
        }
        self.confirm.reserve()
    }

    pub(crate) fn detach_publish(&mut self, tag: u64) {
        if !self.confirm_enabled {
            return;
        }
        let tracker = &mut self.confirm;
        let Some(pending) = tracker.get_mut(tag) else {
            return;
        };
        if pending.outcome.is_some() {
            tracker.remove(tag);
        } else {
            pending.detached = true;
            pending.waker.clear();
        }
    }

    pub(crate) fn cancel_publish(&mut self, tag: u64) {
        if self.confirm_enabled {
            self.confirm.remove(tag);
        }
    }

    pub(crate) fn poll_publish(
        &mut self,
        tag: u64,
        waker: Waker<'d>,
    ) -> Result<Option<PublishConfirm>, Error> {
        if !self.confirm_enabled {
            return Err(Error::Protocol("publisher confirms not enabled"));
        }
        let tracker = &mut self.confirm;
        let Some(pending) = tracker.get_mut(tag) else {
            return Err(Error::Protocol("unknown publish delivery tag"));
        };
        if let Some(outcome) = pending.outcome.take() {
            tracker
                .remove(tag)
                .ok_or(Error::Protocol("publisher confirm tracker is inconsistent"))?;
            return outcome.map(Some);
        }
        if pending.waker.register(waker) {
            Ok(None)
        } else {
            Err(Error::WaiterCapacity)
        }
    }

    pub(in crate::protocol) fn confirm_publishes(
        &mut self,
        tag: u64,
        multiple: bool,
        outcome: PublishConfirm,
    ) -> Result<(), Error> {
        if !self.confirm_enabled {
            return Ok(());
        }
        self.confirm.confirm(tag, multiple, outcome)
    }
}

impl Drop for ChannelState<'_> {
    fn drop(&mut self) {
        self.clear_messages();
    }
}
