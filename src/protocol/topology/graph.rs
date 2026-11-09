use o3::depot::arena::slab;

use crate::protocol::topology::records::{self, exchange, queue};
use crate::protocol::topology::{Local, Record, Table};
use crate::protocol::wire::text::short;

pub(crate) struct Graph<'a, 'c, 'b, 'd, const ID: u8> {
    local: &'a Local<'d, ID>,
    topology: &'a Table<'c, 'd, ID>,
    values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
}

impl<'a, 'c, 'b, 'd, const ID: u8> Graph<'a, 'c, 'b, 'd, ID> {
    pub(crate) const fn new(
        local: &'a Local<'d, ID>,
        topology: &'a Table<'c, 'd, ID>,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
    ) -> Self {
        Self {
            local,
            topology,
            values,
        }
    }

    pub(crate) const fn head(&self) -> (bool, Option<records::Qos>) {
        (self.local.head.confirms, self.local.head.qos)
    }

    pub(crate) const fn current_state(&self) -> &queue::Current<'d, ID> {
        &self.local.current
    }

    pub(crate) fn has_queues(&self) -> bool {
        !self.values.is_empty()
    }

    pub(crate) fn has_replay(&self) -> bool {
        let (confirms, qos) = self.head();
        confirms
            || qos.is_some()
            || self.exchange(0).is_some()
            || !self.values.is_empty()
            || self.queue_bind(0).is_some()
            || self.binding(0).is_some()
    }

    pub(crate) fn exchange(&self, index: usize) -> Option<(usize, &exchange::Declare<'d>)> {
        let record = self.topology.exchanges.store.get(index)?;
        Some((index + 1, record))
    }

    pub(crate) fn queue(
        &self,
        after: Option<queue::Cursor<'d, ID>>,
    ) -> Option<(
        queue::Cursor<'d, ID>,
        queue::Key<'d, ID>,
        Record<'c, 'b, 'd>,
    )> {
        let mut cursor = match after {
            Some(cursor) => cursor,
            None => queue::Cursor {
                next: 0,
                current: self.local.current.key(),
                current_replayed: false,
            },
        };
        while cursor.next < self.values.capacity() {
            let index = cursor.next;
            cursor.next += 1;
            let Some(entry) = self.values.entry_at(index as u32) else {
                continue;
            };
            let key = queue::Key::from_parts(entry.key(), self.topology.queues.owner);
            if Some(key) == cursor.current {
                continue;
            }
            return Some((cursor, key, entry));
        }
        if cursor.current_replayed {
            return None;
        }
        cursor.current_replayed = true;
        let key = cursor.current?;
        let queue = self.values.entry(key.into_parts().0.parts())?;
        Some((cursor, key, queue))
    }

    pub(crate) fn current(&self) -> Option<(queue::Key<'d, ID>, Record<'c, 'b, 'd>)> {
        let key = self.local.current.key()?;
        Some((key, self.values.entry(key.into_parts().0.parts())?))
    }

    pub(crate) fn queue_bind(&self, index: usize) -> Option<(usize, &queue::Bind<'d, ID>)> {
        let record = self.topology.bindings.queues.get(index)?;
        Some((index + 1, record))
    }

    pub(crate) fn binding(&self, index: usize) -> Option<(usize, &exchange::Bind<'d>)> {
        let record = self.topology.bindings.exchanges.get(index)?;
        Some((index + 1, record))
    }

    pub(crate) fn queue_entry(
        &self,
        queue: Option<queue::Key<'d, ID>>,
        name: short::Short<'_>,
    ) -> Option<Record<'c, 'b, 'd>> {
        let key = queue.or_else(|| self.queue_key(name))?;
        self.values.entry(key.into_parts().0.parts())
    }

    pub(crate) fn queue_key(&self, name: short::Short<'_>) -> Option<queue::Key<'d, ID>> {
        if name.is_empty() {
            return match &self.local.current {
                queue::Current::Tracked(key) => {
                    self.values.entry(key.into_parts().0.parts()).map(|_| *key)
                }
                queue::Current::Named { name, .. } => {
                    self.topology.queues.key(self.values, name.name().into())
                }
                queue::Current::None => None,
            };
        }
        self.topology.queues.key(self.values, name)
    }
}
