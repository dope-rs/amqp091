use std::{marker, mem};

use o3::buffer::resident::{Budget, Exact};
use o3::mem::rc;
use schema::{Proof as _, Stored as _, Suffixed as _};

use crate::failure::wire::Allocation;
use crate::failure::{self};
use crate::protocol::wire::text::{self, resident, short};
use crate::protocol::wire::{self, schema};

/// One exact, driver-accounted ownership island for a replayable method.
pub(crate) struct Bytes<'d, L: schema::Layout> {
    pub(in crate::protocol) bytes: Exact<'d>,
    proof: <L::Proof as schema::Proof>::Retained,
    layout: marker::PhantomData<fn() -> L>,
}

pub(crate) struct Patch<'a, L: schema::Layout> {
    fields: [Option<short::Short<'a>>; 3],
    layout: marker::PhantomData<fn() -> L>,
}

impl<L: schema::Layout> Default for Patch<'_, L> {
    fn default() -> Self {
        Self {
            fields: [None; 3],
            layout: marker::PhantomData,
        }
    }
}

impl<'a, L: schema::Layout> Patch<'a, L> {
    pub(crate) fn with<const INDEX: usize>(
        mut self,
        field: schema::Field<L, INDEX>,
        value: short::Short<'a>,
    ) -> Self {
        const {
            assert!(schema::MAX_FIELDS == 3);
            assert!(INDEX < schema::MAX_FIELDS);
            assert!(INDEX < <L::Proof as schema::Proof>::FIELDS);
        };
        // SAFETY: the const assertions prove this field belongs to this patch.
        *unsafe { self.fields.get_unchecked_mut(field.index()) } = Some(value);
        self
    }
}

impl<'d, L: schema::Layout> Bytes<'d, L> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        encoded: wire::Encoded<'_, L>,
    ) -> Result<Self, failure::Error> {
        let proof = encoded.proof().retained();
        let encoded = encoded.as_slice();
        let bytes = Exact::try_copy(budget, encoded)
            .map_err(|error| failure::Error::resident(Allocation::Topology, error))?;
        Ok(Self {
            bytes,
            proof,
            layout: marker::PhantomData,
        })
    }

    pub(crate) fn field<const INDEX: usize>(
        &self,
        field: schema::Field<L, INDEX>,
    ) -> short::Short<'_> {
        let range = Range::new(self.bytes.as_slice(), self.offset(field));
        let bytes = range.as_slice();
        // SAFETY: the sealed encoder emitted these exact value bytes from a
        // live Short, and Bytes retains their immutable exact copy.
        unsafe { short::raw::Encoded::new(bytes) }.bind()
    }

    pub(crate) fn try_field<'b, const INDEX: usize>(
        &self,
        field: schema::Field<L, INDEX>,
    ) -> Result<resident::Short<'b, 'd>, rc::Error> {
        let range = Range::new(self.bytes.as_slice(), self.offset(field));
        let bytes = self.bytes.try_clone()?;
        // SAFETY: Bytes carries the exact immutable owner and encoder-minted
        // offset together; neither can be constructed outside this boundary.
        Ok(unsafe { text::raw::Capsule::from_raw(bytes, range.start, range.len) }.into())
    }

    fn offset<const INDEX: usize>(&self, field: schema::Field<L, INDEX>) -> schema::Offset {
        const { assert!(INDEX < <L::Proof as schema::Proof>::FIELDS) };
        // SAFETY: the const assertion proves the retained proof has this slot.
        unsafe { *self.proof.offsets().get_unchecked(field.index()) }
    }

    pub(crate) fn write_with<S: wire::Put>(&self, out: &mut S, patch: Patch<'_, L>) {
        let bytes = self.bytes.as_slice();
        let mut copied = 0usize;
        for (offset, value) in self.proof.offsets().iter().zip(patch.fields) {
            let Some(value) = value else {
                continue;
            };
            let range = Range::new(bytes, *offset);
            if value.value().as_bytes() == range.as_slice() {
                continue;
            }
            out.extend(&bytes[copied..range.prefix()]);
            out.put_short(value);
            copied = range.end();
        }
        out.extend(&bytes[copied..]);
    }
}

impl<'d, L: schema::Layout> Bytes<'d, L>
where
    <L::Proof as schema::Proof>::Retained: schema::Suffixed,
{
    pub(crate) fn suffix(&self) -> &[u8] {
        let start = self.proof.start().get() as usize;
        // SAFETY: capture copied the validated owner and its suffix proof together.
        unsafe { self.bytes.as_slice().get_unchecked(start..) }
    }
}

pub(crate) struct Field<'d> {
    bytes: Exact<'d>,
}

impl<'d> Field<'d> {
    pub(crate) fn capture(
        budget: &Budget<'d>,
        field: short::Short<'_>,
    ) -> Result<Self, failure::Error> {
        let len = field.len();
        let bytes = Exact::try_slices(budget, [&[len], field.value().as_bytes()])
            .map_err(|error| failure::Error::resident(Allocation::Topology, error))?;
        Ok(Self { bytes })
    }

    pub(crate) fn value(&self) -> short::Short<'_> {
        let range = Range::new(self.bytes.as_slice(), schema::Offset::default());
        let bytes = range.as_slice();
        // SAFETY: Field copied these exact value bytes from a live Short.
        unsafe { short::raw::Encoded::new(bytes) }.bind()
    }
}

struct Range<'a> {
    bytes: &'a [u8],
    start: u32,
    len: u8,
}

impl<'a> Range<'a> {
    fn new(bytes: &'a [u8], offset: schema::Offset) -> Self {
        let prefix = offset.get() as usize;
        // SAFETY: callers carry either a validated schema proof or an owned
        // one-field encoding constructed in this module.
        let len = unsafe { *bytes.get_unchecked(prefix) };
        let start = offset.get() + 1;
        Self { bytes, start, len }
    }

    fn prefix(&self) -> usize {
        (self.start - 1) as usize
    }

    fn end(&self) -> usize {
        self.start as usize + usize::from(self.len)
    }

    fn as_slice(&self) -> &'a [u8] {
        // SAFETY: construction validates this range against this immutable borrow.
        unsafe { self.bytes.get_unchecked(self.start as usize..self.end()) }
    }
}

const _: () = assert!(mem::size_of::<Field<'static>>() == mem::size_of::<Exact<'static>>());
const _: () = {
    assert!(
        mem::size_of::<Bytes<'static, schema::queues::Declaration>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Bytes<'static, schema::queues::Binding>>() == 3 * mem::size_of::<usize>()
    );
};
