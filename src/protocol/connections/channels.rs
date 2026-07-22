use super::super::*;

pub(in crate::protocol) struct ChannelSlot<'d> {
    id: u32,
    next: u32,
    state: ChannelState<'d>,
}

pub(in crate::protocol) struct ChannelPool<'d> {
    by_id: Box<[u32]>,
    slots: Box<[ChannelSlot<'d>]>,
    free: u32,
}

pub(in crate::protocol) struct Channels<'d> {
    pool: UnsafeCell<ChannelPool<'d>>,
}

impl<'d> Channels<'d> {
    pub(in crate::protocol) fn new(
        config: &Config,
        budget: ByteBudgetHandle<'static>,
        body_pool: NonNull<BodyPool>,
    ) -> Self {
        let id_capacity = if config.channel_max == 0 {
            u16::MAX as usize + 1
        } else {
            config.channel_max as usize + 1
        };
        let capacity = config.channel_capacity.min(id_capacity.saturating_sub(1));
        let limits = Limits::from(config);
        Self {
            pool: UnsafeCell::new(ChannelPool {
                by_id: (0..id_capacity).map(|_| NONE).collect(),
                slots: (0..capacity)
                    .map(|index| ChannelSlot {
                        id: NONE,
                        next: if index == 0 { NONE } else { index as u32 - 1 },
                        state: ChannelState::new(limits, budget, body_pool),
                    })
                    .collect(),
                free: if capacity == 0 {
                    NONE
                } else {
                    capacity as u32 - 1
                },
            }),
        }
    }

    pub(in crate::protocol) fn with_pool_mut<R>(
        &self,
        f: impl for<'a> FnOnce(&'a mut ChannelPool<'d>) -> R,
    ) -> R {
        f(unsafe { &mut *self.pool.get() })
    }

    pub(in crate::protocol) fn acquire(&self, id: u16) -> bool {
        self.with_pool_mut(|pool| {
            let Some(mapped) = pool.by_id.get_mut(id as usize) else {
                return false;
            };
            if *mapped != NONE || pool.free == NONE {
                return false;
            }
            let index = pool.free;
            let slot = &mut pool.slots[index as usize];
            pool.free = slot.next;
            slot.id = u32::from(id);
            slot.next = NONE;
            *mapped = index;
            true
        })
    }

    pub(in crate::protocol) fn with_channel_mut<R>(
        &self,
        id: u16,
        f: impl for<'a> FnOnce(&'a mut ChannelState<'d>) -> R,
    ) -> Option<R> {
        self.with_pool_mut(|pool| {
            let index = *pool.by_id.get(id as usize)?;
            (index != NONE)
                .then(|| unsafe { f(&mut pool.slots.get_unchecked_mut(index as usize).state) })
        })
    }

    pub(in crate::protocol) fn release(&self, id: u16) {
        self.with_pool_mut(|pool| {
            let Some(mapped) = pool.by_id.get_mut(id as usize) else {
                return;
            };
            let index = replace(mapped, NONE);
            if index == NONE {
                return;
            }
            let slot = &mut pool.slots[index as usize];
            slot.state.prepare_reuse();
            slot.id = NONE;
            slot.next = pool.free;
            pool.free = index;
        });
    }

    pub(in crate::protocol) fn for_each_mut(&self, mut f: impl FnMut(&mut ChannelState<'d>)) {
        self.with_pool_mut(|pool| {
            for slot in &mut pool.slots {
                if slot.id != NONE {
                    f(&mut slot.state);
                }
            }
        });
    }
}
