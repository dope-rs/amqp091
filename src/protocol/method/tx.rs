use crate::protocol::{class, method, wire};

pub(super) const SELECT: method::Id = method::Id::new(class::TX, 10);
pub(super) const SELECT_OK: method::Id = method::Id::new(class::TX, 11);
pub(super) const COMMIT: method::Id = method::Id::new(class::TX, 20);
pub(super) const COMMIT_OK: method::Id = method::Id::new(class::TX, 21);
pub(super) const ROLLBACK: method::Id = method::Id::new(class::TX, 30);
pub(super) const ROLLBACK_OK: method::Id = method::Id::new(class::TX, 31);

pub struct Select;

impl method::Spec for Select {
    const ID: method::Id = SELECT;
}

impl method::Args for Select {
    fn write_args<S: wire::Put>(&self, _: &mut S) {}
}

impl method::Request for Select {
    type Response = SelectOk;
}

impl<'d> method::Op<'d> for Select {
    type Output<'b, Consumer> = ();
}

pub struct SelectOk;

impl method::Spec for SelectOk {
    const ID: method::Id = SELECT_OK;
}

impl method::Unit for SelectOk {
    const KIND: method::Kind = method::Kind::TxSelect;
}

pub struct Commit;

impl method::Spec for Commit {
    const ID: method::Id = COMMIT;
}

impl method::Args for Commit {
    fn write_args<S: wire::Put>(&self, _: &mut S) {}
}

impl method::Request for Commit {
    type Response = CommitOk;
}

impl<'d> method::Op<'d> for Commit {
    type Output<'b, Consumer> = ();
}

pub struct CommitOk;

impl method::Spec for CommitOk {
    const ID: method::Id = COMMIT_OK;
}

impl method::Unit for CommitOk {
    const KIND: method::Kind = method::Kind::TxCommit;
}

pub struct Rollback;

impl method::Spec for Rollback {
    const ID: method::Id = ROLLBACK;
}

impl method::Args for Rollback {
    fn write_args<S: wire::Put>(&self, _: &mut S) {}
}

impl method::Request for Rollback {
    type Response = RollbackOk;
}

impl<'d> method::Op<'d> for Rollback {
    type Output<'b, Consumer> = ();
}

pub struct RollbackOk;

impl method::Spec for RollbackOk {
    const ID: method::Id = ROLLBACK_OK;
}

impl method::Unit for RollbackOk {
    const KIND: method::Kind = method::Kind::TxRollback;
}
