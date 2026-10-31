use crate::protocol::wire::schema::{Field, Identity, Layout};

#[derive(Clone, Copy)]
pub(crate) struct Bind;

impl Bind {
    pub(crate) const DESTINATION: Field<Self, 0> = Field::new();
    pub(crate) const SOURCE: Field<Self, 1> = Field::new();
    pub(crate) const ROUTING_KEY: Field<Self, 2> = Field::new();
}

impl Layout for Bind {
    type Proof = Identity<3>;
}
