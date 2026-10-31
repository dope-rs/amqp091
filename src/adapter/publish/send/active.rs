use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::publish::send::method::{Kind, Mode};
use crate::adapter::publish::send::stream;
use crate::adapter::step;
use crate::protocol::wire::{Writer, frame};

pub(super) struct Active<
    'body,
    'o,
    'r: 'o,
    'c: 'r,
    'b: 'c,
    'd: 'b,
    const CONFIRM: bool,
    const ID: u8,
> where
    Kind<CONFIRM>: Mode<'o, 'd, ID>,
{
    pub(super) stream: stream::Stream<'o, 'r, 'c, 'b, 'd, CONFIRM, ID>,
    remaining: frame::Body<'body>,
    credit: queue::Registration<'o, 'd, fifo::Scope<'c>>,
}

pub(super) enum Next<'c> {
    More,
    Last(stream::Last<'c>),
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'body, 'd: 'b, const CONFIRM: bool, const ID: u8>
    Active<'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>
where
    Kind<CONFIRM>: Mode<'o, 'd, ID>,
{
    pub(super) const fn new(
        stream: stream::Stream<'o, 'r, 'c, 'b, 'd, CONFIRM, ID>,
        body: frame::Body<'body>,
        credit: queue::Registration<'o, 'd, fifo::Scope<'c>>,
    ) -> Self {
        Self {
            stream,
            remaining: body,
            credit,
        }
    }

    pub(super) fn poll(
        &mut self,
        cx: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> step::Step<'c, 'b, 'd, Next<'c>, ID> {
        let route = self.stream.route();
        let (body, tail) = self.remaining.split(self.stream.max());
        let encode = |output: &mut Writer| {
            frame::Encoder::new(output).body(route.lease().id(), body);
        };
        let result = match tail {
            None => self
                .stream
                .poll_last(cx, encode, &mut self.credit, wake)
                .map(Next::Last),
            Some(_) => self
                .stream
                .poll_more(cx, encode, &mut self.credit, wake)
                .map(|()| Next::More),
        };
        if matches!(result, step::Step::Ready(_, _))
            && let Some(tail) = tail
        {
            self.remaining = tail;
        }
        result
    }
}
