use crate::protocol::wire::schema::{self, Field, Offset};

#[derive(Clone, Copy)]
pub(crate) struct Layout;

impl Layout {
    pub(crate) const NAME: Field<Self, 0> = Field::new();
}

impl schema::Layout for Layout {
    type Proof = [Offset; 1];
}
