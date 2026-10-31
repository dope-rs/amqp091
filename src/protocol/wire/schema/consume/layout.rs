use crate::protocol::wire::schema::{self, Field, Offset};

#[derive(Clone, Copy)]
pub(crate) struct Layout;

impl Layout {
    pub(crate) const QUEUE: Field<Self, 0> = Field::new();
    pub(crate) const CONSUMER_TAG: Field<Self, 1> = Field::new();
}

impl schema::Layout for Layout {
    type Proof = [Offset; 2];
}
