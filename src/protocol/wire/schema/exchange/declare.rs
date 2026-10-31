use crate::protocol::wire::schema::{Field, Layout, Offset};

#[derive(Clone, Copy)]
pub(crate) struct Declare;

impl Declare {
    pub(crate) const NAME: Field<Self, 0> = Field::new();
    pub(crate) const KIND: Field<Self, 1> = Field::new();
}

impl Layout for Declare {
    type Proof = [Offset; 2];
}
