use super::core::{ClassId, ClientMethod, Method, Tx};
use crate::error::Error;
use crate::wire::reader::Reader;
use crate::wire::sink::Sink;

pub struct Select;

impl Method for Select {
    const CLASS_ID: u16 = ClassId::TX;
    const METHOD_ID: u16 = Tx::SELECT;
}

impl ClientMethod for Select {
    fn write_args<S: Sink>(&self, _out: &mut S) {}
}

#[derive(Debug)]
pub struct SelectOk;

impl Method for SelectOk {
    const CLASS_ID: u16 = ClassId::TX;
    const METHOD_ID: u16 = Tx::SELECT_OK;
}

impl SelectOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct Commit;

impl Method for Commit {
    const CLASS_ID: u16 = ClassId::TX;
    const METHOD_ID: u16 = Tx::COMMIT;
}

impl ClientMethod for Commit {
    fn write_args<S: Sink>(&self, _out: &mut S) {}
}

#[derive(Debug)]
pub struct CommitOk;

impl Method for CommitOk {
    const CLASS_ID: u16 = ClassId::TX;
    const METHOD_ID: u16 = Tx::COMMIT_OK;
}

impl CommitOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

pub struct Rollback;

impl Method for Rollback {
    const CLASS_ID: u16 = ClassId::TX;
    const METHOD_ID: u16 = Tx::ROLLBACK;
}

impl ClientMethod for Rollback {
    fn write_args<S: Sink>(&self, _out: &mut S) {}
}

#[derive(Debug)]
pub struct RollbackOk;

impl Method for RollbackOk {
    const CLASS_ID: u16 = ClassId::TX;
    const METHOD_ID: u16 = Tx::ROLLBACK_OK;
}

impl RollbackOk {
    pub fn read(_reader: &mut Reader<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}
