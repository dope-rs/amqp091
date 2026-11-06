use std::mem;

use dope_amqp091::adapter::{channel, conn, method};
use dope_amqp091::failure;
use dope_amqp091::protocol::method::Op;
use dope_amqp091::protocol::method::channel::Open;
use o3::base::cell::{local, region};

use crate::runtime::{self, ready};

pub(crate) struct Call<'c, 'd, const ID: u8, M> {
    lease: channel::Lease<'c, 'd, ID>,
    method: M,
}

impl<'c, 'd, const ID: u8, M> Call<'c, 'd, ID, M> {
    pub(crate) const fn new(lease: channel::Lease<'c, 'd, ID>, method: M) -> Self {
        Self { lease, method }
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8, M>
    ready::Start<'app, 'o, 'r, 'c, 'b, 'd, B, ID> for Call<'c, 'd, ID, M>
where
    M: Op<'d>,
{
    type Run = method::Call<'o, 'r, 'c, 'b, 'd, ID, M>;

    fn start(
        self,
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        access: local::Access<'_, region::Identity<'d>>,
        ready: conn::Ready<'d, ID>,
    ) -> Result<Self::Run, failure::Runtime<'c, 'b, 'd, ID>> {
        handle
            .adapter
            .methods()
            .reserve(access, ready, self.lease, self.method)
    }
}

const _: () = {
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
                Call<'static, 'static, 0, Open>,
            >,
        >() == 11 * mem::size_of::<usize>()
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
                Call<'static, 'static, 0, Open>,
            >,
        >() == 11 * mem::size_of::<usize>()
    );
};
