use std::cell::{Cell, UnsafeCell};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::mem::transmute;
use std::ptr::NonNull;

use o3::buffer::Shared;
use o3::mem::ByteLease;

pub mod properties;

const NONE: u32 = u32::MAX;

struct BodyNode {
    value: UnsafeCell<Option<Shared>>,
    next: Cell<u32>,
}

struct BodyPoolFree {
    free: Box<[u32]>,
    free_len: usize,
}

pub(crate) struct BodyPool {
    nodes: Box<[BodyNode]>,
    free: UnsafeCell<BodyPoolFree>,
}

impl BodyPool {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        assert!(
            u32::try_from(capacity).is_ok(),
            "body fragment capacity overflow"
        );
        Self {
            nodes: (0..capacity)
                .map(|_| BodyNode {
                    value: UnsafeCell::new(None),
                    next: Cell::new(NONE),
                })
                .collect(),
            free: UnsafeCell::new(BodyPoolFree {
                free: (0..capacity as u32).rev().collect(),
                free_len: capacity,
            }),
        }
    }

    fn insert(&self, value: Shared) -> Result<u32, Shared> {
        let free = unsafe { &mut *self.free.get() };
        if free.free_len == 0 {
            return Err(value);
        }
        free.free_len -= 1;
        let index = unsafe { *free.free.get_unchecked(free.free_len) };
        let node = unsafe { self.nodes.get_unchecked(index as usize) };
        unsafe { *node.value.get() = Some(value) };
        node.next.set(NONE);
        Ok(index)
    }

    fn set_next(&self, index: u32, next: u32) {
        unsafe { self.nodes.get_unchecked(index as usize) }
            .next
            .set(next);
    }

    fn get(&self, index: u32) -> (&Shared, u32) {
        let node = unsafe { self.nodes.get_unchecked(index as usize) };
        (
            unsafe { (&*node.value.get()).as_ref().unwrap_unchecked() },
            node.next.get(),
        )
    }

    fn take(&self, index: u32) -> (Shared, u32) {
        let node = unsafe { self.nodes.get_unchecked(index as usize) };
        let next = node.next.replace(NONE);
        let value = unsafe { (&mut *node.value.get()).take().unwrap_unchecked() };
        let free = unsafe { &mut *self.free.get() };
        unsafe { *free.free.get_unchecked_mut(free.free_len) = index };
        free.free_len += 1;
        (value, next)
    }
}

pub struct Body<'d> {
    first: Option<Shared>,
    rest_head: u32,
    rest_tail: u32,
    len: usize,
    budget: Option<ByteLease<'d>>,
    pool: NonNull<BodyPool>,
    lifetime: PhantomData<&'d BodyPool>,
}

impl<'d> Body<'d> {
    pub(crate) fn new(pool: &'d BodyPool) -> Self {
        Self {
            first: None,
            rest_head: NONE,
            rest_tail: NONE,
            len: 0,
            budget: None,
            pool: NonNull::from(pool),
            lifetime: PhantomData,
        }
    }

    fn pool(&self) -> &BodyPool {
        unsafe { self.pool.as_ref() }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_slice(&self) -> Option<&[u8]> {
        if self.rest_head == NONE {
            return Some(self.first.as_ref().map_or(&[], Shared::as_slice));
        }
        None
    }

    pub fn chunks(&self) -> Chunks<'_> {
        Chunks {
            first: self.first.as_ref(),
            next: self.rest_head,
            pool: self.pool(),
        }
    }

    pub fn starts_with(&self, prefix: &[u8]) -> bool {
        if prefix.len() > self.len {
            return false;
        }
        self.chunks()
            .flat_map(|chunk| chunk.iter().copied())
            .take(prefix.len())
            .eq(prefix.iter().copied())
    }

    pub fn copy_to_vec(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(self.len);
        for chunk in self.chunks() {
            body.extend_from_slice(chunk);
        }
        body
    }

    pub(crate) fn push(&mut self, chunk: Shared) -> Result<(), Shared> {
        let len = chunk.len();
        if self.first.is_none() {
            self.first = Some(chunk);
        } else {
            let index = self.pool().insert(chunk)?;
            if self.rest_tail == NONE {
                self.rest_head = index;
            } else {
                self.pool().set_next(self.rest_tail, index);
            }
            self.rest_tail = index;
        }
        self.len += len;
        Ok(())
    }

    pub(crate) fn reserve(&mut self, budget: ByteLease<'d>) {
        debug_assert!(self.budget.is_none());
        self.budget = Some(budget);
    }

    pub(crate) unsafe fn assume_lifetime<'a>(self) -> Body<'a> {
        unsafe { transmute::<Body<'d>, Body<'a>>(self) }
    }
}

impl Debug for Body<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Body").field("len", &self.len).finish()
    }
}

impl Drop for Body<'_> {
    fn drop(&mut self) {
        let mut next = self.rest_head;
        while next != NONE {
            let (value, following) = self.pool().take(next);
            drop(value);
            next = following;
        }
    }
}

pub struct Chunks<'a> {
    first: Option<&'a Shared>,
    next: u32,
    pool: &'a BodyPool,
}

impl<'a> Iterator for Chunks<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first) = self.first.take() {
            return Some(first.as_slice());
        }
        if self.next == NONE {
            return None;
        }
        let (value, next) = self.pool.get(self.next);
        self.next = next;
        Some(value.as_slice())
    }
}

impl PartialEq<[u8]> for Body<'_> {
    fn eq(&self, other: &[u8]) -> bool {
        self.len == other.len()
            && self
                .chunks()
                .flat_map(|chunk| chunk.iter().copied())
                .eq(other.iter().copied())
    }
}
