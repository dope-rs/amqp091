use std::{mem, pin, task};

use dope::core::driver::schedule::ready::wake::fanout;
use dope_amqp091::adapter::conn;
use dope_amqp091::failure;
use dope_fiber::context;
use o3::base::cell::local;

use crate::runtime;

pub trait Source<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8>:
    Sized
{
    fn handle(&self) -> runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>;

    fn poll(
        &mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<Result<conn::Ready<'d, ID>, failure::Runtime<'c, 'b, 'd, ID>>>;
}

pub struct Exact<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8> {
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    ready: conn::Ready<'d, ID>,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Exact<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(crate) const fn new(
        handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
        ready: conn::Ready<'d, ID>,
    ) -> Self {
        Self { handle, ready }
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Source<'app, 'o, 'r, 'c, 'b, 'd, B, ID> for Exact<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    fn handle(&self) -> runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID> {
        self.handle
    }

    fn poll(
        &mut self,
        _: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<Result<conn::Ready<'d, ID>, failure::Runtime<'c, 'b, 'd, ID>>> {
        task::Poll::Ready(Ok(self.ready))
    }
}

pub struct Auto<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'app + 'b, B: ?Sized, const ID: u8> {
    handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>,
    wait: fanout::Registration<'o, 'd>,
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Auto<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    pub(crate) const fn new(handle: runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID>) -> Self {
        Self {
            handle,
            wait: fanout::Registration::new(),
        }
    }
}

impl<'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'app, 'd: 'app + 'b, B: ?Sized, const ID: u8>
    Source<'app, 'o, 'r, 'c, 'b, 'd, B, ID> for Auto<'app, 'o, 'r, 'c, 'b, 'd, B, ID>
{
    fn handle(&self) -> runtime::Handle<'app, 'o, 'r, 'c, 'b, 'd, B, ID> {
        self.handle
    }

    fn poll(
        &mut self,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<Result<conn::Ready<'d, ID>, failure::Runtime<'c, 'b, 'd, ID>>> {
        let wake = context.as_ref().completion();
        let access = local::Access::new(context.as_mut().region_token());
        self.handle
            .adapter
            .connection()
            .observe()
            .poll_ready(access, &mut self.wait, wake)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Exact<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
            == mem::size_of::<
                runtime::Handle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
            >() + mem::size_of::<conn::Ready<'static, 0>>()
    );
    assert!(
        mem::size_of::<Auto<'static, 'static, 'static, 'static, 'static, 'static, (), 0>>()
            == mem::size_of::<
                runtime::Handle<'static, 'static, 'static, 'static, 'static, 'static, (), 0>,
            >() + mem::size_of::<fanout::Registration<'static, 'static>>()
    );
};
