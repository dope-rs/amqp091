use crate::protocol::wire::schema::{Field, Identity, Layout, Offset};

#[derive(Clone, Copy)]
pub(crate) struct Binding;

impl Binding {
    pub(crate) const QUEUE: Field<Self, 0> = Field::new();
    pub(crate) const EXCHANGE: Field<Self, 1> = Field::new();
    pub(crate) const ROUTING_KEY: Field<Self, 2> = Field::new();
}

impl Layout for Binding {
    type Proof = Identity<3>;
}

#[derive(Clone, Copy)]
pub(crate) struct Declaration;

impl Declaration {
    pub(crate) const QUEUE: Field<Self, 0> = Field::new();
}

impl Layout for Declaration {
    type Proof = [Offset; 1];
}
