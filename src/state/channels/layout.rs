use std::{alloc, cell, mem};

use o3::alloc::Overflow;
use o3::depot::index::bitmap;

use crate::state::channels::table::Slot;
use crate::state::channels::{Index, Lease};

#[repr(C)]
pub(super) struct Entry<'r, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    pub(super) key: Option<Lease<'d, ID>>,
    pub(super) next: Option<Index<'c, ID>>,
    pub(super) value: Slot<'r, 'c, 'b, 'd, ID>,
}

#[derive(Clone, Copy)]
pub(super) struct Layout {
    pub(super) allocation: alloc::Layout,
    pub(super) shape: bitmap::Shape<u32>,
    pub(super) physical: u16,
    pub(super) bitmap_offset: usize,
    pub(super) entries_offset: usize,
}

impl Layout {
    pub(super) fn new<'r, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>(
        logical: usize,
        physical: u32,
    ) -> Result<Self, Overflow> {
        if logical > usize::from(u16::MAX) + 1 {
            return Err(Overflow);
        }
        if physical > u32::from(u16::MAX) {
            return Err(Overflow);
        }
        let physical = physical as u16;
        let shape = bitmap::Shape::<u32>::new(logical as u32);
        let map = alloc::Layout::array::<Option<Index<'c, ID>>>(logical).map_err(|_| Overflow)?;
        let bits =
            alloc::Layout::array::<cell::Cell<usize>>(shape.words()).map_err(|_| Overflow)?;
        let (plan, bit_at) = map.extend(bits).map_err(|_| Overflow)?;
        let rows = alloc::Layout::array::<Entry<'r, 'c, 'b, 'd, ID>>(usize::from(physical))
            .map_err(|_| Overflow)?;
        let (plan, row_at) = plan.extend(rows).map_err(|_| Overflow)?;
        let plan = plan.pad_to_align();
        if plan.size() > u32::MAX as usize {
            return Err(Overflow);
        }
        Ok(Self {
            allocation: plan,
            shape,
            physical,
            bitmap_offset: bit_at,
            entries_offset: row_at,
        })
    }

    pub(super) fn align<'r, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>() -> usize {
        mem::align_of::<Option<Index<'c, ID>>>()
            .max(mem::align_of::<cell::Cell<usize>>())
            .max(mem::align_of::<Entry<'r, 'c, 'b, 'd, ID>>())
    }
}
