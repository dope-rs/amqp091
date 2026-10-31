use crate::protocol::{class, method, wire};

pub(super) const SELECT: method::Id = method::Id::new(class::CONFIRM, 10);
pub(super) const SELECT_OK: method::Id = method::Id::new(class::CONFIRM, 11);

pub struct Select;

impl method::Spec for Select {
    const ID: method::Id = SELECT;
}

impl method::Args for Select {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_bits([false]);
    }
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
    const KIND: method::Kind = method::Kind::Confirm;
}
