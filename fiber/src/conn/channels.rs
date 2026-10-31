use std::mem;

use dope_amqp091::adapter::conn;
use dope_amqp091::failure;
use dope_fiber::abi;

use crate::channel;
use crate::channel::access::open;
use crate::runtime::{self, ready};

/// Channel creation facet for one connection route.
#[repr(transparent)]
pub struct Channels<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8>
{
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
}

impl<B: ?Sized, const ID: u8> Copy for Channels<'_, '_, '_, '_, '_, '_, B, ID> {}

impl<B: ?Sized, const ID: u8> Clone for Channels<'_, '_, '_, '_, '_, '_, B, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Channels<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(super) const fn new(handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>) -> Self {
        Self { handle }
    }

    pub fn open(
        self,
        id: u16,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            channel::Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
            failure::Runtime<'c, 'b, 'd, ID>,
        >,
    > {
        ready::Op::new(ready::Auto::new(self.handle), open::Open::new(id))
    }

    pub fn open_in(
        self,
        target: conn::Ready<'d, ID>,
        id: u16,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            channel::Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID, conn::Ready<'d, ID>>,
            failure::Runtime<'c, 'b, 'd, ID>,
        >,
    > {
        ready::Op::new(ready::Exact::new(self.handle, target), open::Bound::new(id))
    }

    pub fn open_auto(
        self,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            channel::Channel<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
            failure::Runtime<'c, 'b, 'd, ID>,
        >,
    > {
        ready::Op::new(ready::Auto::new(self.handle), open::Auto)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Channels<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
            == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<
            ready::Op<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                (),
                0,
                ready::Auto<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
                open::Open,
            >,
        >() == 17 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<
            ready::Op<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                (),
                0,
                ready::Auto<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
                open::Auto,
            >,
        >() == 17 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<
            ready::Op<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                (),
                0,
                ready::Exact<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
                open::Bound,
            >,
        >() == 19 * mem::size_of::<usize>()
    );
};
