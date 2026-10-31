use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::publish::send::method::{Kind, Mode};
use crate::adapter::publish::send::{active, stream};
use crate::adapter::step;
use crate::message::property;
use crate::protocol::wire::{Writer, frame};

pub(super) struct Header<
    'properties,
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
    properties: &'properties property::Properties,
    body: &'body [u8],
    credit: queue::Registration<'o, 'd, fifo::Scope<'c>>,
}

pub(super) enum Next<'body, 'c> {
    Body(frame::Body<'body>),
    Last(stream::Last<'c>),
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'properties, 'body, 'd: 'b, const CONFIRM: bool, const ID: u8>
    Header<'properties, 'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID>
where
    Kind<CONFIRM>: Mode<'o, 'd, ID>,
{
    pub(super) const fn new(
        stream: stream::Stream<'o, 'r, 'c, 'b, 'd, CONFIRM, ID>,
        props: &'properties property::Properties,
        body: &'body [u8],
        credit: queue::Registration<'o, 'd, fifo::Scope<'c>>,
    ) -> Self {
        Self {
            stream,
            properties: props,
            body,
            credit,
        }
    }

    pub(super) fn poll(
        &mut self,
        cx: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> step::Step<'c, 'b, 'd, Next<'body, 'c>, ID> {
        let route = self.stream.route();
        let encode = |output: &mut Writer| {
            frame::Encoder::new(output).write(frame::Type::HEADER, route.lease().id(), |sink| {
                property::Encoder::new(self.properties).encode(sink, self.body.len() as u64);
            });
        };
        match frame::Body::new(self.body) {
            None => self
                .stream
                .poll_last(cx, encode, &mut self.credit, wake)
                .map(Next::Last),
            Some(body) => self
                .stream
                .poll_more(cx, encode, &mut self.credit, wake)
                .map(|()| Next::Body(body)),
        }
    }

    pub(super) fn body(
        self,
        body: frame::Body<'body>,
    ) -> active::Active<'body, 'o, 'r, 'c, 'b, 'd, CONFIRM, ID> {
        active::Active::new(self.stream, body, self.credit)
    }
}
