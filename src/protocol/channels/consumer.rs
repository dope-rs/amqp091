use super::super::*;

pub(in crate::protocol) struct ConsumerSlot<'d> {
    pub(in crate::protocol) tag: String,
    pub(in crate::protocol) head: Option<SlabKey>,
    pub(in crate::protocol) tail: Option<SlabKey>,
    pub(in crate::protocol) waker: WakeSlot<'d>,
    pub(in crate::protocol) canceled: bool,
}

pub(in crate::protocol) struct DeliveryNode {
    pub(in crate::protocol) value: Delivery<'static>,
    pub(in crate::protocol) next: Option<SlabKey>,
}

pub(in crate::protocol) struct ConsumerTable<'d> {
    slots: Slab<ConsumerSlot<'d>>,
    buckets: FixedHashTable<SlabKey>,
}

impl<'d> ConsumerTable<'d> {
    pub(in crate::protocol) fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Slab::with_capacity(capacity),
            buckets: FixedHashTable::with_capacity(capacity),
        }
    }

    pub(in crate::protocol) fn key(&self, tag: &str) -> Option<SlabKey> {
        let hash = hash_tag(tag);
        self.buckets
            .get(hash, |key| {
                self.slots
                    .get(*key)
                    .is_some_and(|consumer| consumer.tag == tag)
            })
            .copied()
    }

    pub(in crate::protocol) fn get(&self, key: SlabKey) -> Option<&ConsumerSlot<'d>> {
        self.slots.get(key)
    }

    pub(in crate::protocol) fn get_mut(&mut self, key: SlabKey) -> Option<&mut ConsumerSlot<'d>> {
        self.slots.get_mut(key)
    }

    pub(in crate::protocol) fn insert(
        &mut self,
        consumer: ConsumerSlot<'d>,
    ) -> Result<SlabKey, Error> {
        let hash = hash_tag(&consumer.tag);
        let key = self
            .slots
            .insert(consumer)
            .map_err(|_| Error::CapacityOverflow("consumer"))?;
        if self.buckets.try_insert(hash, key, |_| false).is_err() {
            self.slots.remove(key);
            return Err(Error::CapacityOverflow("consumer index"));
        }
        Ok(key)
    }

    pub(in crate::protocol) fn remove(&mut self, key: SlabKey) -> Option<ConsumerSlot<'d>> {
        let consumer = self.slots.get(key)?;
        let hash = hash_tag(&consumer.tag);
        self.buckets.remove(hash, |stored| *stored == key)?;
        self.slots.remove(key)
    }

    pub(in crate::protocol) fn values_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut ConsumerSlot<'d>> {
        self.slots.values_mut()
    }

    pub(in crate::protocol) fn clear(&mut self) {
        self.slots.clear();
        self.buckets.clear();
    }

    pub(in crate::protocol) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

fn hash_tag(tag: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for byte in tag.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}
