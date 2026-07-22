use super::core::{ClassId, ClientMethod, Confirm, Method};
use crate::error::Error;
use crate::wire::put::Put;
use crate::wire::reader::Reader;
use crate::wire::sink::Sink;

pub struct Select {
    pub no_wait: bool,
}

impl Method for Select {
    const CLASS_ID: u16 = ClassId::CONFIRM;
    const METHOD_ID: u16 = Confirm::SELECT;
}

impl ClientMethod for Select {
    fn write_args<S: Sink>(&self, out: &mut S) {
        out.put_bits(&[self.no_wait]);
    }
}

#[derive(Debug)]
pub struct SelectOk;

impl Method for SelectOk {
    const CLASS_ID: u16 = ClassId::CONFIRM;
    const METHOD_ID: u16 = Confirm::SELECT_OK;
}

impl SelectOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}
