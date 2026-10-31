use std::marker;

use schema::Identified as _;

use crate::failure::wire::Fault;
use crate::protocol::wire::outputs::{Detached, Fields};
use crate::protocol::wire::text::short;
use crate::protocol::wire::{Identity, Writer, frame, schema};

#[must_use = "captured method arguments carry the only validated field provenance"]
pub(crate) struct Encoded<'a, L: schema::Layout> {
    bytes: &'a [u8],
    proof: L::Proof,
    layout: marker::PhantomData<fn() -> L>,
}

impl<'a, L: schema::Layout> Encoded<'a, L> {
    pub(in crate::protocol::wire) fn new(bytes: &'a [u8], proof: L::Proof) -> Result<Self, Fault> {
        schema::Proof::validate(&proof, bytes.len())?;
        let mut last = 0usize;
        for (field, offset) in schema::Proof::offsets(&proof).iter().enumerate() {
            let prefix = offset.get() as usize;
            let Some(&length) = bytes.get(prefix) else {
                return Err(Fault::EncodedField {
                    field: field as u8,
                    offset: offset.get(),
                    length: 0,
                    available: bytes.len(),
                });
            };
            let end = prefix + 1 + usize::from(length);
            if end > bytes.len() {
                return Err(Fault::EncodedField {
                    field: field as u8,
                    offset: offset.get(),
                    length,
                    available: bytes.len(),
                });
            }
            if prefix < last {
                return Err(Fault::EncodedOrder {
                    field: field as u8,
                    previous: last,
                    actual: offset.get(),
                });
            }
            last = end;
        }
        Ok(Self {
            bytes,
            proof,
            layout: marker::PhantomData,
        })
    }

    pub(crate) fn as_slice(&self) -> &'a [u8] {
        self.bytes
    }

    pub(crate) fn proof(&self) -> L::Proof {
        self.proof
    }

    pub(crate) fn field<const INDEX: usize>(
        &self,
        field: schema::Field<L, INDEX>,
    ) -> short::Short<'a> {
        const { assert!(INDEX < <L::Proof as schema::Proof>::FIELDS) };
        // SAFETY: the const assertion proves this schema field has an offset.
        let offset = unsafe { *schema::Proof::offsets(&self.proof).get_unchecked(field.index()) };
        let prefix = offset.get() as usize;
        // SAFETY: new proves this prefix and its u8-sized payload are in the
        // same immutable owner; Field can select only an offset in that proof.
        let length = unsafe { *self.bytes.get_unchecked(prefix) } as usize;
        let start = prefix + 1;
        let end = start + length;
        // SAFETY: new validates the full range, and the sealed encoder emitted
        // these exact value bytes from a live Short.
        let bytes = unsafe { self.bytes.get_unchecked(start..end) };
        unsafe { short::raw::Encoded::new(bytes) }.bind()
    }

    pub(crate) fn method_frame(
        output: &mut Writer,
        channel: u16,
        id: [u8; 4],
        write: impl for<'output> FnOnce(Fields<'output, L>) -> Fields<'output, L, L::Proof>,
    ) -> Detached<L> {
        let (start, end, proof) =
            frame::Encoder::new(&mut *output).method_with(channel, id, |output| {
                let start = output.position();
                let proof = write(Fields::new(output)).finish();
                (start, output.position(), proof)
            });
        Detached::new(start, end, proof)
    }
}

impl<'a, L: schema::Layout> Encoded<'a, L>
where
    L::Proof: schema::Identified,
{
    pub(crate) fn identity(&self) -> Identity<'a> {
        let proof = self.proof();
        let start = proof.start().get() as usize;
        // SAFETY: new validates this suffix against the same immutable borrow.
        let bytes = unsafe { self.as_slice().get_unchecked(start..) };
        Identity::new(bytes, proof.fingerprint())
    }
}
