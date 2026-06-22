use super::encode::ClientMethod;
use super::spec::{ClassId, Confirm, Method};
use crate::Error;
use crate::wire::{Bits, Reader, Sink};

pub struct Select {
    pub no_wait: bool,
}

impl Method for Select {
    const CLASS_ID: u16 = ClassId::CONFIRM;
    const METHOD_ID: u16 = Confirm::SELECT;
}

impl ClientMethod for Select {
    fn write_args<S: Sink>(&self, out: &mut S) {
        Bits::pack(out, &[self.no_wait]);
    }
}

#[derive(Debug)]
pub struct SelectOk;

impl Method for SelectOk {
    const CLASS_ID: u16 = ClassId::CONFIRM;
    const METHOD_ID: u16 = Confirm::SELECT_OK;
}

impl SelectOk {
    pub fn read(_r: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}
