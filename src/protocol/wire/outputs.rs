use std::marker;

use o3::buffer::write::consume;

use crate::failure::wire::Fault;
use crate::message::field;
use crate::protocol::wire::text::short;
use crate::protocol::wire::{self, Encoded, Put as _, Write, Writer, schema};

#[must_use = "detached method provenance must be joined to its validated frame"]
pub(crate) struct Detached<L: schema::Layout> {
    start: u32,
    end: u32,
    proof: L::Proof,
}

impl<L: schema::Layout> Detached<L> {
    pub(in crate::protocol::wire) const fn new(start: u32, end: u32, proof: L::Proof) -> Self {
        Self { start, end, proof }
    }

    pub(crate) fn attach(self, frame: &[u8]) -> Result<Encoded<'_, L>, Fault> {
        let Some(bytes) = frame.get(self.start as usize..self.end as usize) else {
            return Err(Fault::EncodedRange {
                start: self.start,
                end: self.end,
                available: frame.len(),
            });
        };
        Encoded::new(bytes, self.proof)
    }
}

pub(crate) struct Fields<'a, L: schema::Layout, P = [schema::Offset; 0]> {
    output: &'a mut Writer,
    base: u32,
    proof: P,
    layout: marker::PhantomData<fn() -> L>,
}

impl<'a, L: schema::Layout> Fields<'a, L> {
    pub(in crate::protocol::wire) fn new(output: &'a mut Writer) -> Self {
        Self {
            base: output.position(),
            output,
            proof: [],
            layout: marker::PhantomData,
        }
    }
}

impl<L: schema::Layout> Fields<'_, L, L::Proof> {
    pub(in crate::protocol::wire) fn finish(self) -> L::Proof {
        self.proof
    }
}

impl<'a, L: schema::Layout, P: schema::Append<schema::Offset> + Copy> Fields<'a, L, P> {
    pub(crate) fn put_field<const INDEX: usize>(
        self,
        _field: schema::Field<L, INDEX>,
        value: short::Short<'_>,
    ) -> Fields<'a, L, P::Next> {
        const {
            assert!(INDEX == P::LEN);
            assert!(INDEX < <L::Proof as schema::Proof>::FIELDS);
        };
        let offset = write_field(self.output, self.base, value);
        let proof = self.proof.append(offset);
        self.advance(proof)
    }
}

fn write_field(output: &mut Writer, base: u32, value: short::Short<'_>) -> schema::Offset {
    let offset = schema::Offset::new(output.position() - base);
    Plain::new(output).put_short(value);
    offset
}

impl<'a, const N: usize, L: schema::Layout<Proof = schema::Identity<N>>>
    Fields<'a, L, [schema::Offset; N]>
{
    pub(crate) fn put_identity(self, value: &field::Table) -> Fields<'a, L, L::Proof> {
        let offset = self.offset();
        value.encode(&mut Plain::new(self.output));
        let proof = schema::Identity::new(self.proof, offset);
        self.advance(proof)
    }
}

impl<'a, L: schema::Layout, P> Fields<'a, L, P> {
    pub(crate) fn put_table(&mut self, value: &field::Table) {
        value.encode(&mut Plain::new(self.output));
    }

    fn offset(&self) -> schema::Offset {
        schema::Offset::new(self.output.position() - self.base)
    }

    fn advance<T>(self, proof: T) -> Fields<'a, L, T> {
        Fields {
            output: self.output,
            base: self.base,
            proof,
            layout: self.layout,
        }
    }
}

impl<L: schema::Layout, P> Write for Fields<'_, L, P> {
    fn len(&self) -> usize {
        self.output.len()
    }

    fn status(&self) -> Result<(), consume::Error> {
        self.output.status()
    }

    fn push(&mut self, byte: u8) {
        self.output.push(byte);
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.output.extend(bytes);
    }
}

impl<L: schema::Layout, P> wire::Put for Fields<'_, L, P> {}

pub(crate) struct Plain<'a, S: ?Sized> {
    output: &'a mut S,
}

impl<'a, S: ?Sized> Plain<'a, S> {
    pub(crate) fn new(output: &'a mut S) -> Self {
        Self { output }
    }
}

impl<S: Write + ?Sized> Write for Plain<'_, S> {
    fn len(&self) -> usize {
        self.output.len()
    }

    fn status(&self) -> Result<(), consume::Error> {
        self.output.status()
    }

    fn push(&mut self, byte: u8) {
        self.output.push(byte);
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.output.extend(bytes);
    }
}

impl<S: Write + ?Sized> wire::Put for Plain<'_, S> {}
