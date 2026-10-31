use dope_amqp091::adapter::conn;
use dope_amqp091::failure;
use dope_fiber::abi;
use o3::base::cell::local;

use crate::runtime;

/// Graceful-close command facet.
#[repr(transparent)]
pub struct Control<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8> {
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
}

impl<B: ?Sized, const ID: u8> Copy for Control<'_, '_, '_, '_, '_, '_, B, ID> {}

impl<B: ?Sized, const ID: u8> Clone for Control<'_, '_, '_, '_, '_, '_, B, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Control<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(super) const fn new(handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>) -> Self {
        Self { handle }
    }

    /// Admits a graceful close for one exact ready generation.
    pub fn start<'text>(
        self,
        ready: conn::Ready<'d, ID>,
        code: u16,
        text: &'text str,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'text
    where
        'd: 'text,
        'app: 'text,
    {
        let mut start = self
            .handle
            .adapter
            .connection()
            .control()
            .start(ready, code, text);
        abi::poll_fn(move |mut cx| {
            let wake = cx.as_ref().completion();
            let local = local::Access::new(cx.as_mut().region_token());
            start.poll(local, wake)
        })
    }

    /// Admits a graceful close and waits for that generation's `close-ok`.
    pub fn close<'text>(
        self,
        ready: conn::Ready<'d, ID>,
        code: u16,
        text: &'text str,
    ) -> impl abi::Fiber<'d, Output = Result<(), failure::Runtime<'c, 'b, 'd, ID>>> + 'text
    where
        'd: 'text,
        'app: 'text,
    {
        let mut close = self
            .handle
            .adapter
            .connection()
            .control()
            .close(ready, code, text);
        abi::poll_fn(move |mut cx| {
            let wake = cx.as_ref().completion();
            let local = local::Access::new(cx.as_mut().region_token());
            close.poll(local, wake)
        })
    }
}

const _: () = assert!(
    std::mem::size_of::<Control<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
        == std::mem::size_of::<usize>()
);
