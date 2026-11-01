use std::{num, task};

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::consumer::{Error, Origin};
use crate::adapter::core::egress;
use crate::failure;
use crate::message::ack::Disposition;
use crate::protocol::method::{self, basic};

#[derive(Clone, Copy)]
enum State {
    Active(Disposition),
    Done,
}

/// One disposition bound to the exact connection epoch and channel that
/// delivered its tag.
pub struct Settle<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    source: Origin<'o, 'r, 'c, 'b, 'd, ID>,
    delivery_tag: num::NonZero<u64>,
    credit: queue::Registration<'o, 'd, fifo::Scope<'c>>,
    state: State,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Settle<'o, 'r, 'c, 'b, 'd, ID> {
    pub(super) const fn new(
        source: Origin<'o, 'r, 'c, 'b, 'd, ID>,
        tag: num::NonZero<u64>,
        kind: Disposition,
    ) -> Self {
        Self {
            source,
            delivery_tag: tag,
            credit: queue::Registration::new(),
            state: State::Active(kind),
        }
    }

    pub fn poll(
        &mut self,
        access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), Error<'c, 'b, 'd, ID>>> {
        let kind = match self.state {
            State::Active(kind) => kind,
            State::Done => {
                return task::Poll::Ready(Err(Error::from_runtime(failure::Runtime::from(
                    failure::Error::Polled,
                ))));
            }
        };
        let source = &self.source;
        let tag = self.delivery_tag;
        let result = egress::Egress::new(source.core()).poll(
            access,
            source.epoch().raw(),
            source.lease(),
            |output| match kind {
                Disposition::Ack => method::Encode::write(
                    &basic::Ack {
                        delivery_tag: tag.get(),
                        multiple: false,
                    },
                    output,
                    source.lease().id(),
                ),
                Disposition::Nack { requeue: retry } => method::Encode::write(
                    &basic::Nack {
                        delivery_tag: tag.get(),
                        multiple: false,
                        requeue: retry,
                    },
                    output,
                    source.lease().id(),
                ),
                Disposition::Reject { requeue: retry } => method::Encode::write(
                    &basic::Reject {
                        delivery_tag: tag,
                        requeue: retry,
                    },
                    output,
                    source.lease().id(),
                ),
            },
            &mut self.credit,
            wake,
        );
        match result {
            task::Poll::Pending => task::Poll::Pending,
            task::Poll::Ready(result) => {
                self.state = State::Done;
                match result {
                    Ok(()) => task::Poll::Ready(Ok(())),
                    Err(error) => task::Poll::Ready(Err(Error::from_runtime(error))),
                }
            }
        }
    }
}

const _: () = {
    assert!(size_of::<State>() == size_of::<Disposition>());
    assert!(
        size_of::<Settle<'static, 'static, 'static, 'static, 'static, 0>>()
            == size_of::<[usize; 8]>()
    );
};
