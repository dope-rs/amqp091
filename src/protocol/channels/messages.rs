use super::super::*;

pub(crate) trait ChannelMessages<'d> {
    fn consumer_next(
        &mut self,
        key: SlabKey,
        wake: CompletionWaker<'d>,
    ) -> Result<Poll<Option<Delivery<'static>>>, Error>;
    fn pop_delivery(&mut self, consumer_key: SlabKey) -> Option<Delivery<'static>>;
    fn complete_head_with(
        &mut self,
        expect: ExpectMethod,
        response: PendingResponse,
    ) -> Result<Option<Outbound>, Error>;
    fn route_delivery(&mut self, delivery: Delivery<'static>) -> Result<(), Error>;
    fn route_return(&mut self, returned: Returned<'static>) -> Result<(), Error>;
    fn next_return(
        &mut self,
        wake: CompletionWaker<'d>,
    ) -> Result<Option<Returned<'static>>, Error>;
    fn cancel_consumer(&mut self, tag: &str) -> Result<(), Error>;
    fn detach_consumer(&mut self, key: SlabKey);
    fn fail_all(&mut self, error: &Error);
}

impl<'d> ChannelMessages<'d> for ChannelState<'d> {
    fn consumer_next(
        &mut self,
        key: SlabKey,
        wake: CompletionWaker<'d>,
    ) -> Result<Poll<Option<Delivery<'static>>>, Error> {
        if let Some(delivery) = self.pop_delivery(key) {
            return Ok(Poll::Ready(Some(delivery)));
        }
        let Some(consumer) = self.consumers.get_mut(key) else {
            return Ok(Poll::Ready(None));
        };
        if consumer.canceled {
            return Ok(Poll::Ready(None));
        }
        if consumer.waker.register(wake) {
            Ok(Poll::Pending)
        } else {
            Err(Error::WaiterCapacity)
        }
    }

    fn pop_delivery(&mut self, consumer_key: SlabKey) -> Option<Delivery<'static>> {
        let consumer = self.consumers.get_mut(consumer_key)?;
        let head = consumer.head?;
        let node = self.deliveries.remove(head)?;
        consumer.head = node.next;
        if consumer.head.is_none() {
            consumer.tail = None;
        }
        Some(node.value)
    }

    fn complete_head_with(
        &mut self,
        expect: ExpectMethod,
        response: PendingResponse,
    ) -> Result<Option<Outbound>, Error> {
        let Some(&key) = self.op_order.front() else {
            return Ok(None);
        };
        let detached = {
            let Some(head) = self.pending_ops.get_mut(key) else {
                return Err(Error::Protocol("pending method order is inconsistent"));
            };
            if head.expect != expect {
                return Err(Error::Protocol("response method does not match pending op"));
            }
            if !head.detached {
                head.outcome = Some(Ok(response));
                head.waker.wake();
            }
            head.detached
        };
        self.op_order.pop_front();
        if detached {
            self.pending_ops.remove(key);
        }
        while let Some(&next) = self.op_order.front() {
            let Some(op) = self.pending_ops.get_mut(next) else {
                self.op_order.pop_front();
                continue;
            };
            op.sent = true;
            return Ok(op.frame.take());
        }
        Ok(None)
    }

    fn route_delivery(&mut self, d: Delivery<'static>) -> Result<(), Error> {
        let Some(consumer_key) = self.consumers.key(&d.method.consumer_tag) else {
            return Ok(());
        };
        let node_key = match self.deliveries.insert(DeliveryNode {
            value: d,
            next: None,
        }) {
            Ok(key) => key,
            Err(_) => return Err(Error::CapacityOverflow("delivery")),
        };
        let Some(tail) = self
            .consumers
            .get(consumer_key)
            .map(|consumer| consumer.tail)
        else {
            self.deliveries.remove(node_key);
            return Err(Error::Protocol("consumer index is inconsistent"));
        };
        if let Some(tail) = tail {
            let Some(node) = self.deliveries.get_mut(tail) else {
                self.deliveries.remove(node_key);
                return Err(Error::Protocol("consumer delivery queue is inconsistent"));
            };
            node.next = Some(node_key);
        }
        let Some(consumer) = self.consumers.get_mut(consumer_key) else {
            self.deliveries.remove(node_key);
            return Err(Error::Protocol("consumer index is inconsistent"));
        };
        consumer.head.get_or_insert(node_key);
        consumer.tail = Some(node_key);
        consumer.waker.wake();
        Ok(())
    }

    fn route_return(&mut self, r: Returned<'static>) -> Result<(), Error> {
        if self.returns.len() >= self.limits.returns {
            return Err(Error::CapacityOverflow("return"));
        }
        self.returns
            .push_back(r)
            .map_err(|_| Error::CapacityOverflow("return"))?;
        self.return_waker.wake();
        Ok(())
    }

    fn next_return(
        &mut self,
        wake: CompletionWaker<'d>,
    ) -> Result<Option<Returned<'static>>, Error> {
        if self.phase != ChannelPhase::Open {
            return Err(Error::Closed);
        }
        if let Some(r) = self.returns.pop_front() {
            return Ok(Some(r));
        }
        if self.return_waker.register(wake) {
            Ok(None)
        } else {
            Err(Error::WaiterCapacity)
        }
    }

    fn cancel_consumer(&mut self, tag: &str) -> Result<(), Error> {
        if let Some(key) = self.consumers.key(tag) {
            let Some(consumer) = self.consumers.get_mut(key) else {
                return Err(Error::Protocol("consumer index is inconsistent"));
            };
            consumer.canceled = true;
            consumer.waker.wake();
        }
        Ok(())
    }

    fn detach_consumer(&mut self, key: SlabKey) {
        let Some(mut consumer) = self.consumers.remove(key) else {
            return;
        };
        while let Some(head) = consumer.head {
            let Some(delivery) = self.deliveries.remove(head) else {
                break;
            };
            consumer.head = delivery.next;
        }
    }

    fn fail_all(&mut self, err: &Error) {
        while let Some(key) = self.op_order.pop_front() {
            let Some(op) = self.pending_ops.get_mut(key) else {
                continue;
            };
            if op.detached {
                self.pending_ops.remove(key);
            } else {
                op.outcome = Some(Err(err.clone()));
                op.waker.wake();
            }
        }
        for c in self.consumers.values_mut() {
            c.canceled = true;
            c.waker.wake();
        }
        if self.confirm_enabled {
            let tracker = &mut self.confirm;
            tracker.head = NONE;
            tracker.tail = NONE;
            for slot in &mut tracker.slots {
                slot.prev = NONE;
                slot.next = NONE;
                if slot
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.detached)
                {
                    slot.pending = None;
                    tracker.len -= 1;
                } else if let Some(pending) = &mut slot.pending {
                    if pending.outcome.is_none() {
                        pending.outcome = Some(Err(err.clone()));
                    }
                    pending.waker.wake();
                }
            }
        }
        self.return_waker.wake();
    }
}
