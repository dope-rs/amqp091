use std::alloc::Layout;
use std::{marker, mem, num, ptr, slice};

use bitmap::Access as _;
use o3::depot::index::bitmap::{self, Fill, entry};
use o3::{alloc, depot};

use crate::failure::{self, wire};
use crate::permits;
use crate::state::channels::layout::{self, Entry};
use crate::state::channels::table::Slot;
use crate::state::channels::{Error, Index, Lease};

#[repr(C)]
pub(super) struct Store<'r, 'c, 'b, 'd, const ID: u8> {
    pub(super) memory: Memory<'r, 'c, 'b, 'd, ID>,
    pub(super) shape: bitmap::Shape<u32>,
    bytes: u32,
    physical: Option<Index<'c, ID>>,
}

/// One vacant channel bound to exclusive access to its owning store.
pub(super) struct Vacancy<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    entry: entry::Row<&'a mut Store<'r, 'c, 'b, 'd, ID>, (), entry::Occupied>,
}

#[repr(C)]
pub(super) struct Memory<'r, 'c, 'b, 'd, const ID: u8> {
    pointer: ptr::NonNull<u8>,
    entries_offset: usize,
    marker: marker::PhantomData<(Lease<'d, ID>, Slot<'r, 'c, 'b, 'd, ID>)>,
}

struct Init<'r, 'c, 'b, 'd, const ID: u8> {
    pointer: ptr::NonNull<u8>,
    layout: layout::Layout,
    initialized: Option<Index<'c, ID>>,
    marker: marker::PhantomData<(Lease<'d, ID>, Slot<'r, 'c, 'b, 'd, ID>)>,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Store<'r, 'c, 'b, 'd, ID> {
    pub(super) fn try_new(
        layout: layout::Layout,
        permits: &permits::Registry<'c, 'd>,
        mut make: impl FnMut(Index<'c, ID>) -> Result<Slot<'r, 'c, 'b, 'd, ID>, alloc::Error>,
    ) -> Result<Self, Error> {
        let mut init = Init::new(layout)?;
        while init.len() < layout.physical {
            init.push(permits, &mut make)?;
        }
        let mut store = init.finish();
        if let Some(zero) = store.first(()) {
            zero.remove();
        }
        Ok(store)
    }

    pub(super) const fn slots(&self) -> u16 {
        match self.physical {
            Some(index) => index.position().get(),
            None => 0,
        }
    }

    pub(super) const fn last(&self) -> Option<Index<'c, ID>> {
        self.physical
    }

    pub(super) fn capacity(&self, upper: u16) -> u16 {
        self.slots()
            .min(upper)
            .min(self.shape.bits().saturating_sub(1) as u16)
    }

    pub(super) fn vacant(
        &mut self,
        id: u16,
    ) -> Result<Vacancy<'_, 'r, 'c, 'b, 'd, ID>, failure::Error> {
        let entry = entry::Row::try_new(self, usize::from(id))
            .map_err(|error| failure::Error::Protocol(wire::Fault::ChannelBounds(error)))?;
        let entry = entry
            .occupied(())
            .map_err(|absent| failure::Error::ChannelInUse(absent.index() as u16))?;
        Ok(Vacancy { entry })
    }

    pub(super) fn first_vacant(&mut self) -> Option<Vacancy<'_, 'r, 'c, 'b, 'd, ID>> {
        Some(Vacancy {
            entry: self.first(())?,
        })
    }

    pub(super) fn key_at(&self, id: u16) -> Option<Lease<'d, ID>> {
        if usize::from(id) >= self.shape.bits() {
            return None;
        }
        let index = unsafe { *self.memory.map().add(usize::from(id)) };
        unsafe { self.memory.entry(index?).key }
    }

    pub(super) fn occupied(
        &self,
        index: Index<'c, ID>,
    ) -> Option<(Lease<'d, ID>, &Slot<'r, 'c, 'b, 'd, ID>)> {
        if index.offset() >= self.slots() {
            return None;
        }
        let entry = unsafe { self.memory.entry(index) };
        Some((entry.key?, &entry.value))
    }

    pub(super) fn occupied_mut(
        &mut self,
        index: Index<'c, ID>,
    ) -> Option<(Lease<'d, ID>, &mut Slot<'r, 'c, 'b, 'd, ID>)> {
        if index.offset() >= self.slots() {
            return None;
        }
        let entry = unsafe { self.memory.entry_mut(index) };
        Some((entry.key?, &mut entry.value))
    }

    pub(super) fn value_mut(&mut self, index: Index<'c, ID>) -> &mut Slot<'r, 'c, 'b, 'd, ID> {
        &mut unsafe { self.memory.entry_mut(index) }.value
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Vacancy<'_, 'r, 'c, 'b, 'd, ID> {
    pub(super) fn id(&self) -> u16 {
        self.entry.index() as u16
    }

    pub(super) fn occupy<R>(
        self,
        index: Index<'c, ID>,
        key: Lease<'d, ID>,
        init: impl FnOnce() -> Result<R, failure::Error>,
    ) -> Result<(Option<Index<'c, ID>>, R), failure::Error> {
        let result = init()?;
        let mut removed = self.entry.remove();
        let id = removed.index();
        // SAFETY: only the matching map and physical entry change; the bitmap owner stays fixed.
        let store = unsafe { depot::raw::Rows::source_mut(&mut removed) };
        unsafe {
            store.memory.map_mut().add(id).write(Some(index));
            let entry = store.memory.entry_mut(index);
            let next = entry.next;
            entry.key = Some(key);
            entry.next = None;
            Ok((next, result))
        }
    }
}

impl<const ID: u8> Drop for Store<'_, '_, '_, '_, ID> {
    fn drop(&mut self) {
        // SAFETY: `bytes` is retained from the exact successful layout and the
        // field types determine the same allocation alignment for this `ID`.
        let layout = unsafe {
            Layout::from_size_align_unchecked(self.bytes as usize, layout::Layout::align::<ID>())
        };
        unsafe {
            let entries = self
                .memory
                .pointer
                .as_ptr()
                .add(self.memory.entries_offset)
                .cast::<Entry<'_, '_, '_, '_, ID>>();
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                entries,
                usize::from(self.slots()),
            ));
        }
        unsafe { alloc::raw::LayoutExt::deallocate(layout, self.memory.pointer) };
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Memory<'r, 'c, 'b, 'd, ID> {
    pub(super) unsafe fn map(&self) -> *const Option<Index<'c, ID>> {
        self.pointer.as_ptr().cast()
    }

    pub(super) unsafe fn map_mut(&mut self) -> *mut Option<Index<'c, ID>> {
        self.pointer.as_ptr().cast()
    }

    pub(super) unsafe fn entry(&self, index: Index<'c, ID>) -> &Entry<'r, 'c, 'b, 'd, ID> {
        unsafe {
            &*self
                .pointer
                .as_ptr()
                .add(self.entries_offset)
                .cast::<Entry<'r, 'c, 'b, 'd, ID>>()
                .add(usize::from(index.offset()))
        }
    }

    pub(super) unsafe fn entry_mut(
        &mut self,
        index: Index<'c, ID>,
    ) -> &mut Entry<'r, 'c, 'b, 'd, ID> {
        unsafe {
            &mut *self
                .pointer
                .as_ptr()
                .add(self.entries_offset)
                .cast::<Entry<'r, 'c, 'b, 'd, ID>>()
                .add(usize::from(index.offset()))
        }
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Init<'r, 'c, 'b, 'd, ID> {
    fn new(layout: layout::Layout) -> Result<Self, alloc::Error> {
        let pointer = alloc::raw::LayoutExt::try_allocate(layout.allocation)?;
        let mut init = Self {
            pointer,
            layout,
            initialized: None,
            marker: marker::PhantomData,
        };
        unsafe {
            for id in 0..layout.shape.bits() {
                init.map().add(id).write(None);
            }
            for (word, value) in layout.shape.fill(Fill::Full).enumerate() {
                init.bitmap().add(word).write(value);
            }
        }
        Ok(init)
    }

    fn push(
        &mut self,
        permits: &permits::Registry<'c, 'd>,
        make: &mut impl FnMut(Index<'c, ID>) -> Result<Slot<'r, 'c, 'b, 'd, ID>, alloc::Error>,
    ) -> Result<(), Error> {
        let position = self.initialized.map_or(num::NonZero::<u16>::MIN, |index| {
            index.position().saturating_add(1)
        });
        let index = Index::try_new(
            permits
                .try_index(usize::from(position.get()))
                .map_err(Error::Lane)?,
        )
        .map_err(Error::Index)?;
        let value = make(index)?;
        unsafe {
            self.entries()
                .add(usize::from(index.offset()))
                .write(Entry {
                    key: None,
                    next: self.initialized,
                    value,
                });
        }
        self.initialized = Some(index);
        Ok(())
    }

    fn len(&self) -> u16 {
        self.initialized.map_or(0, |index| index.position().get())
    }

    fn finish(self) -> Store<'r, 'c, 'b, 'd, ID> {
        let init = mem::ManuallyDrop::new(self);
        Store {
            memory: Memory {
                pointer: init.pointer,
                entries_offset: init.layout.entries_offset,
                marker: marker::PhantomData,
            },
            shape: init.layout.shape,
            bytes: init.layout.allocation.size() as u32,
            physical: init.initialized,
        }
    }

    unsafe fn map(&mut self) -> *mut Option<Index<'c, ID>> {
        self.pointer.as_ptr().cast()
    }

    unsafe fn bitmap(&mut self) -> *mut bitmap::Word {
        unsafe { self.pointer.as_ptr().add(self.layout.bitmap_offset).cast() }
    }

    unsafe fn entries(&mut self) -> *mut Entry<'r, 'c, 'b, 'd, ID> {
        unsafe { self.pointer.as_ptr().add(self.layout.entries_offset).cast() }
    }
}

// SAFETY: Store owns its initialized map followed by this immutable exact bitmap.
unsafe impl<const ID: u8> depot::raw::Words for Store<'_, '_, '_, '_, ID> {
    type Plane = ();
    type Size = u32;

    fn shape(&self) -> bitmap::Shape {
        self.shape
    }

    fn words(&self, (): ()) -> &[bitmap::Word] {
        if self.shape.bits() == 0 {
            return &[];
        }
        let bytes = self.shape.bits() * mem::size_of::<Option<Index<'_, ID>>>();
        let align = mem::align_of::<bitmap::Word>();
        let offset = (bytes + align - 1) & !(align - 1);
        unsafe {
            slice::from_raw_parts(
                self.memory.pointer.as_ptr().add(offset).cast(),
                self.shape.words(),
            )
        }
    }
    fn row(entry: entry::Row<&Self>, (): ()) -> &[bitmap::Word] {
        let store = *entry.source();
        let bytes = store.shape.bits() * mem::size_of::<Option<Index<'_, ID>>>();
        let align = mem::align_of::<bitmap::Word>();
        let offset = (bytes + align - 1) & !(align - 1);
        // SAFETY: this retained row proves the exact source allocation is nonempty.
        unsafe {
            slice::from_raw_parts(
                store.memory.pointer.as_ptr().add(offset).cast(),
                store.shape.words(),
            )
        }
    }
}

// SAFETY: Store owns this allocation uniquely; exclusive access excludes all bitmap aliases.
unsafe impl<const ID: u8> depot::raw::Bitmap for Store<'_, '_, '_, '_, ID> {}

impl<const ID: u8> Drop for Init<'_, '_, '_, '_, ID> {
    fn drop(&mut self) {
        for offset in 0..self.len() {
            unsafe { ptr::drop_in_place(&mut (*self.entries().add(usize::from(offset))).value) };
        }
        unsafe { alloc::raw::LayoutExt::deallocate(self.layout.allocation, self.pointer) };
    }
}

const _: () = {
    assert!(
        mem::size_of::<Store<'static, 'static, 'static, 'static, 0>>()
            == 4 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Vacancy<'static, 'static, 'static, 'static, 'static, 0>>()
            == 2 * mem::size_of::<usize>()
    );
};
