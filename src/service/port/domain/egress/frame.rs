use o3::buffer::pool;

use crate::failure::{self, wire};
use crate::protocol::wire::Writer;

pub(in crate::service::port::domain) struct Frame<P, T = pool::Cursor> {
    output: T,
    proof: P,
}

impl Frame<(), Writer> {
    pub(super) const fn new(output: Writer) -> Self {
        Self { output, proof: () }
    }

    pub(in crate::service::port::domain) fn encode<P>(
        mut self,
        encode: impl FnOnce(&mut Writer) -> P,
    ) -> Result<Frame<P>, failure::Error> {
        let proof = encode(&mut self.output);
        self.finish(proof)
    }

    pub(in crate::service::port::domain) fn try_encode<P>(
        mut self,
        encode: impl FnOnce(&mut Writer) -> Result<P, failure::Error>,
    ) -> Result<Frame<P>, failure::Error> {
        let proof = encode(&mut self.output)?;
        self.finish(proof)
    }

    fn finish<P>(self, proof: P) -> Result<Frame<P>, failure::Error> {
        let output = self
            .output
            .try_finish()
            .map_err(|error| failure::Error::Capacity(wire::Capacity::Outbound(error)))?;
        Ok(Frame { output, proof })
    }
}

impl Frame<()> {
    pub(in crate::service::port::domain) fn into_output(self) -> pool::Cursor {
        self.output
    }
}

impl<P> Frame<P> {
    pub(in crate::service::port::domain) fn into_parts(self) -> (pool::Cursor, P) {
        (self.output, self.proof)
    }
}

const _: () = assert!(std::mem::size_of::<Frame<()>>() == std::mem::size_of::<pool::Cursor>());
