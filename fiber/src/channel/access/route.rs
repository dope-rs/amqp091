use dope_amqp091::adapter::conn;

use crate::runtime::{self, ready};

/// Route source selected by the channel access typestate.
pub trait Route<'d, const ID: u8>: Copy {
    type Source<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, B: ?Sized>: ready::Source<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
    where
        'd: 'b;

    fn source<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, B: ?Sized>(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    ) -> Self::Source<'app, 'o, 'r, 'c, 'b, B>
    where
        'd: 'b;
}

impl<'d, const ID: u8> Route<'d, ID> for () {
    type Source<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, B: ?Sized>
        = ready::Auto<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
    where
        'd: 'b;

    fn source<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, B: ?Sized>(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    ) -> Self::Source<'app, 'o, 'r, 'c, 'b, B>
    where
        'd: 'b,
    {
        ready::Auto::new(handle)
    }
}

impl<'d, const ID: u8> Route<'d, ID> for conn::Ready<'d, ID> {
    type Source<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, B: ?Sized>
        = ready::Exact<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
    where
        'd: 'b;

    fn source<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, B: ?Sized>(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    ) -> Self::Source<'app, 'o, 'r, 'c, 'b, B>
    where
        'd: 'b,
    {
        ready::Exact::new(handle, self)
    }
}
