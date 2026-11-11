use std::{marker, mem};

use dope::core::driver::route::{KeyTag, lease};
use o3::alloc;
use o3::depot::arena::slab;
use o3::depot::arena::slab::entries;
use o3::depot::index::Initialized;
use o3::depot::map::{self, flat};
use o3::depot::{self, capacity};

use crate::failure::{self, wire};
use crate::permits;
use crate::protocol::topology::records::queue;
use crate::protocol::topology::{Record, Table};
use crate::protocol::wire::Fingerprint;
use crate::protocol::wire::text::short;

type RouteOwner<'d, const ID: u8> = lease::Owner<'d, KeyTag<ID>>;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Owner<'d, const ID: u8> {
    route: marker::PhantomData<fn(&'d RouteOwner<'d, ID>) -> &'d RouteOwner<'d, ID>>,
}

impl<'d, const ID: u8> Owner<'d, ID> {
    pub(in crate::protocol::topology) const fn new() -> Self {
        Self {
            route: marker::PhantomData,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Exchange<'d, const ID: u8>(u32, Owner<'d, ID>);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct QueueBinding<'d, const ID: u8>(u32, Owner<'d, ID>);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Link<'d, const ID: u8>(u32, Owner<'d, ID>);

pub(crate) struct QueueHold<'c, 'b, 'd, const ID: u8> {
    entry: slab::Vacant<
        entries::Pending<'c, queue::Declare<'b, 'd>, { u32::MAX }, entries::Interior<false>>,
        queue::Tag,
    >,
    owner: Owner<'d, ID>,
    scope: marker::PhantomData<fn(&'c ()) -> &'c ()>,
}

/// One outstanding credit in the sole store of this key kind in its port scope.
pub(crate) struct Hold<'c, K: Key> {
    scope: marker::PhantomData<fn(&'c K) -> &'c K>,
}

pub(crate) enum Upsert<K, H> {
    Existing(K),
    Vacant(H),
}

pub(crate) struct Binding<'c, K: Key> {
    pub(super) hash: u64,
    pub(super) entry: Upsert<K, Hold<'c, K>>,
}

pub(crate) trait Release<'c, 'd, const ID: u8> {
    fn release(self, table: &mut Table<'c, 'd, ID>);
}

pub(crate) trait Key: Copy + Eq {
    fn new(raw: u32) -> Self;

    fn get(self) -> usize;
}

pub(crate) struct Store<'c, T, K: Key> {
    values: Vec<T>,
    identities: flat::Table<K>,
    held: usize,
    scope: marker::PhantomData<fn(&'c ()) -> &'c ()>,
}

pub(crate) struct QueueStore<'c, 'd, const ID: u8> {
    names: flat::Table<queue::Key<'d, ID>>,
    pub(super) owner: Owner<'d, ID>,
    scope: marker::PhantomData<fn(&'c ()) -> &'c ()>,
}

pub(super) struct Unindexed<'c, 'b, 'd, const ID: u8> {
    record: Record<'c, 'b, 'd>,
    owner: Owner<'d, ID>,
    scope: marker::PhantomData<fn(&'c ()) -> &'c ()>,
}

impl<'c, 'b, 'd, const ID: u8> Unindexed<'c, 'b, 'd, ID> {
    pub(super) fn key(&self) -> queue::Key<'d, ID> {
        queue::Key::from_parts(self.record.key(), self.owner)
    }

    pub(super) fn value_mut(&mut self) -> &mut queue::Declare<'b, 'd> {
        self.record.value_mut()
    }

    pub(super) fn discard(self) {
        self.record.discard();
    }
}

impl<'d, const ID: u8> Key for Exchange<'d, ID> {
    fn new(raw: u32) -> Self {
        Self(raw, Owner::new())
    }

    fn get(self) -> usize {
        self.0 as usize
    }
}

impl<'d, const ID: u8> Key for QueueBinding<'d, ID> {
    fn new(raw: u32) -> Self {
        Self(raw, Owner::new())
    }

    fn get(self) -> usize {
        self.0 as usize
    }
}

impl<'d, const ID: u8> Key for Link<'d, ID> {
    fn new(raw: u32) -> Self {
        Self(raw, Owner::new())
    }

    fn get(self) -> usize {
        self.0 as usize
    }
}

impl<'c, T, K: Key> Store<'c, T, K> {
    pub(super) fn get(&self, index: usize) -> Option<&T> {
        self.values.get(index)
    }

    pub(super) fn key(&self, hash: u64, matches: impl Fn(&T) -> bool) -> Option<K> {
        self.identities
            .get(hash, |key| self.values.get(key.get()).is_some_and(&matches))
            .copied()
    }

    pub(super) fn try_new<'d>(
        cap: capacity::Count,
        ids: map::Plan,
        _: &permits::Registry<'c, 'd>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            values: <Vec<T> as alloc::VecExt<T>>::try_new(cap.get())?,
            identities: flat::Table::try_new(ids)?,
            held: 0,
            scope: marker::PhantomData,
        })
    }

    pub(super) fn reserve(&mut self) -> Result<Hold<'c, K>, wire::Capacity> {
        if self.values.len() + self.held >= self.values.capacity() {
            return Err(wire::Capacity::TopologyMetadata);
        }
        self.held += 1;
        Ok(Hold {
            scope: marker::PhantomData,
        })
    }

    /// Only reserve creates this noncopyable credit, after incrementing this
    /// scope's sole matching store. Every successful commit or undo consumes it.
    fn release(&mut self, _hold: Hold<'c, K>) {
        self.held -= 1;
    }

    pub(super) fn upsert(
        &mut self,
        plan: Binding<'c, K>,
        value: T,
        same: impl Fn(&T, &T) -> bool,
    ) -> Result<(), (T, Binding<'c, K>, wire::Capacity)> {
        if let Upsert::Existing(key) = &plan.entry
            && let Some(current) = self.values.get_mut(key.get())
            && same(current, &value)
        {
            *current = value;
            return Ok(());
        }
        let entry = self.identities.entry(plan.hash, |key| {
            self.values
                .get(key.get())
                .is_some_and(|current| same(current, &value))
        });
        match entry {
            Some(flat::Entry::Occupied(entry)) => {
                // SAFETY: this occupied matcher admitted the exact dense index in
                // values. The retained field borrows exclude changes to either owner.
                let current = unsafe {
                    depot::raw::Proof::<_, Initialized<()>>::new((
                        self.values.as_mut_slice(),
                        entry.get().get(),
                    ))
                }
                .into();
                *current = value;
            }
            Some(flat::Entry::Vacant(entry)) => {
                if matches!(&plan.entry, Upsert::Existing(_))
                    && self.values.len() + self.held >= self.values.capacity()
                {
                    return Err((value, plan, wire::Capacity::TopologyMetadata));
                }
                let key = K::new(self.values.len() as u32);
                entry.insert(key);
                // SAFETY: the retained hold, or the Existing admission above,
                // proves len < capacity. This store preserves the exact
                // configured capacity established at construction.
                (unsafe { alloc::raw::VecExt::vacancy(&mut self.values) })(value);
            }
            None => return Err((value, plan, wire::Capacity::TopologyMetadata)),
        }
        if let Upsert::Vacant(hold) = plan.entry {
            self.release(hold);
        }
        Ok(())
    }

    pub(super) fn remove(
        &mut self,
        hash: u64,
        matches: impl Fn(&T) -> bool,
        key_fn: impl Fn(&T) -> u64,
    ) -> Option<T> {
        let entry = self
            .identities
            .occupied(hash, |key| self.values.get(key.get()).is_some_and(&matches))?;
        let key = entry.remove();
        Some(self.remove_value(key, key_fn))
    }

    fn remove_value(&mut self, key: K, key_fn: impl Fn(&T) -> u64) -> T {
        use o3::depot::raw::Admit as _;

        let tail = K::new((self.values.len() - 1) as u32);
        // SAFETY: both callers admitted this exact live index while retaining
        // this exclusive store; removing its directory entry did not mutate values.
        let (value, moved) =
            unsafe { depot::raw::Proof::<_, Initialized<()>>::new((&mut self.values, key.get())) }
                .admit();
        if let Some(row) = moved {
            let moved = key_fn(row);
            // SAFETY: this exclusive store owns the dense/index bijection.
            // Removing a different key preserves the tail's exact membership;
            // swap_remove moves its value without changing its canonical hash.
            let (mut entry, ()) = flat::Table::member(unsafe {
                depot::raw::Proof::new((&mut self.identities, (), moved, |stored: &K| {
                    *stored == tail
                }))
            });
            *entry.get_mut() = key;
        }
        value
    }

    pub(super) fn remove_where(
        &mut self,
        mut remove: impl FnMut(&T) -> bool,
        key_fn: impl Fn(&T) -> u64,
    ) {
        let mut index = 0;
        while index < self.values.len() {
            if remove(&self.values[index]) {
                let hash = key_fn(&self.values[index]);
                let key = K::new(index as u32);
                let (entry, ()) = flat::Table::member(unsafe {
                    // SAFETY: the dense element retains its exact directory
                    // membership in this exclusive store throughout removal.
                    depot::raw::Proof::new((&mut self.identities, (), hash, |stored: &K| {
                        *stored == key
                    }))
                });
                entry.discard();
                self.remove_value(key, &key_fn);
            } else {
                index += 1;
            }
        }
    }
}

impl<'c, 'd, const ID: u8> QueueStore<'c, 'd, ID> {
    pub(super) fn try_new(
        names: map::Plan,
        owner: Owner<'d, ID>,
        _: &permits::Registry<'c, 'd>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            names: flat::Table::try_new(names)?,
            owner,
            scope: marker::PhantomData,
        })
    }

    pub(super) fn key<'b>(
        &self,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
        name: short::Short<'_>,
    ) -> Option<queue::Key<'d, ID>> {
        self.key_at(values, name, Fingerprint::of(name.value().as_bytes()).get())
    }

    pub(super) fn key_at<'b>(
        &self,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
        name: short::Short<'_>,
        hash: u64,
    ) -> Option<queue::Key<'d, ID>> {
        self.names
            .get(hash, |key| {
                values
                    .entry(key.into_parts().0.parts())
                    .is_some_and(|entry| {
                        let queue = entry.value();
                        short::Short::from(queue.user_name()) == name
                            || short::Short::from(queue.server_name()) == name
                    })
            })
            .copied()
    }

    pub(super) fn insert<'b>(
        &mut self,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
        hold: QueueHold<'c, 'b, 'd, ID>,
        value: queue::Declare<'b, 'd>,
    ) -> queue::Key<'d, ID> {
        let name = value.server_name();
        let hash = value.hash();
        // SAFETY: this scope's slab and directory share one admitted capacity.
        // Every name refers to a live row, while this retained reservation keeps
        // one slot unpublished: names <= live rows < capacity. The exclusive
        // directory loan preserves that admission through insertion.
        let (entry, hold) = flat::Table::admit(
            unsafe { depot::raw::Proof::new((&mut self.names, hold)) },
            hash,
            |key| {
                values
                    .entry(key.into_parts().0.parts())
                    .is_some_and(|entry| entry.value().server_name() == name)
            },
        );
        match entry {
            flat::Entry::Occupied(entry) => *entry.get(),
            flat::Entry::Vacant(slot) => {
                let QueueHold { entry, owner, .. } = hold;
                let key = queue::Key::from_parts(entry.insert(value), owner);
                slot.insert(key);
                key
            }
        }
    }

    pub(super) fn index<'b>(
        &mut self,
        entry: Unindexed<'c, 'b, 'd, ID>,
        hash: u64,
    ) -> Record<'c, 'b, 'd> {
        let key = entry.key();
        let Unindexed { record, .. } = entry;
        // SAFETY: detach removed this retained busy row's sole name. It cannot
        // be recycled or indexed elsewhere while its exclusive row loan lives.
        // The shared plan bounds names <= live rows - 1 < directory capacity.
        let (_, record) = flat::Table::insert(
            unsafe { depot::raw::Proof::new((&mut self.names, record)) },
            hash,
            key,
        );
        record
    }

    pub(super) fn detach<'b>(
        &mut self,
        record: Record<'c, 'b, 'd>,
        hash: u64,
    ) -> Unindexed<'c, 'b, 'd, ID> {
        let key = queue::Key::from_parts(record.key(), self.owner);
        // SAFETY: callers transfer the same topology's live indexed row and
        // its current name hash. The row loan excludes another detach/remove.
        let (entry, record) = flat::Table::member(unsafe {
            depot::raw::Proof::new((
                &mut self.names,
                record,
                hash,
                |stored: &queue::Key<'d, ID>| *stored == key,
            ))
        });
        entry.discard();
        Unindexed {
            record,
            owner: self.owner,
            scope: self.scope,
        }
    }

    pub(super) fn reserve<'b>(
        &self,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
    ) -> Result<QueueHold<'c, 'b, 'd, ID>, failure::Error> {
        let entry = values
            .vacancy()
            .ok_or_else(|| failure::Error::Capacity(wire::Capacity::TopologyMetadata))?;
        Ok(QueueHold {
            entry,
            owner: self.owner,
            scope: self.scope,
        })
    }
}

impl<'c, 'd, const ID: u8> Release<'c, 'd, ID> for Binding<'c, Exchange<'d, ID>> {
    fn release(self, table: &mut Table<'c, 'd, ID>) {
        if let Upsert::Vacant(hold) = self.entry {
            table.exchanges.store.release(hold);
        }
    }
}

impl<'c, 'd, const ID: u8> Release<'c, 'd, ID> for Binding<'c, QueueBinding<'d, ID>> {
    fn release(self, table: &mut Table<'c, 'd, ID>) {
        if let Upsert::Vacant(hold) = self.entry {
            table.bindings.queues.release(hold);
        }
    }
}

impl<'c, 'd, const ID: u8> Release<'c, 'd, ID> for Binding<'c, Link<'d, ID>> {
    fn release(self, table: &mut Table<'c, 'd, ID>) {
        if let Upsert::Vacant(hold) = self.entry {
            table.bindings.exchanges.release(hold);
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Owner<'static, 0>>() == 0);
    assert!(mem::size_of::<Exchange<'static, 0>>() == mem::size_of::<u32>());
    assert!(mem::align_of::<Exchange<'static, 0>>() == mem::align_of::<u32>());
    assert!(mem::size_of::<Option<Exchange<'static, 0>>>() == mem::size_of::<Option<u32>>());
    assert!(mem::size_of::<Hold<'static, Exchange<'static, 0>>>() == 0);
    assert!(
        mem::size_of::<QueueHold<'static, 'static, 'static, 0>>() == 2 * mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Hold<'static, QueueBinding<'static, 0>>>() == 0);
    assert!(mem::size_of::<Hold<'static, Link<'static, 0>>>() == 0);
    assert!(
        mem::size_of::<Upsert<Exchange<'static, 0>, Hold<'static, Exchange<'static, 0>>>>()
            == mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Binding<'static, Link<'static, 0>>>() == 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Unindexed<'static, 'static, 'static, 0>>() == 2 * mem::size_of::<usize>()
    );
};
