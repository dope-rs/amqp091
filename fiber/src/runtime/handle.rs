use dope::runtime::client;
use dope_amqp091::adapter;

pub struct Handle<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8> {
    pub(crate) adapter: adapter::Adapter<'o, 'r, 'c, 'b, 'd, ID>,
    bound: client::Bound<'app, 'd, B>,
}

impl<B: ?Sized, const ID: u8> Copy for Handle<'_, '_, '_, '_, '_, '_, B, ID> {}

impl<B: ?Sized, const ID: u8> Clone for Handle<'_, '_, '_, '_, '_, '_, B, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(crate) const fn new(
        adapter: adapter::Adapter<'o, 'r, 'c, 'b, 'd, ID>,
        bound: client::Bound<'app, 'd, B>,
    ) -> Self {
        Self { adapter, bound }
    }

    pub(crate) const fn bound(self) -> client::Bound<'app, 'd, B> {
        self.bound
    }
}

const _: () = assert!(
    std::mem::size_of::<Handle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == std::mem::size_of::<usize>()
);
const _: () = assert!(
    std::mem::align_of::<Handle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == std::mem::align_of::<usize>()
);
