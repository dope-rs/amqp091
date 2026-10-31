use std::marker;

#[derive(Clone, Copy)]
pub(crate) struct Field<Schema, const INDEX: usize> {
    schema: marker::PhantomData<fn() -> Schema>,
}

impl<Schema, const INDEX: usize> Field<Schema, INDEX> {
    pub(crate) const fn new() -> Self {
        Self {
            schema: marker::PhantomData,
        }
    }

    pub(crate) const fn index(&self) -> usize {
        INDEX
    }
}

const _: () = assert!(std::mem::size_of::<Field<(), 0>>() == 0);
