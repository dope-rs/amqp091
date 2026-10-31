use std::{mem, task};

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::fanout;
use o3::base::cell::{local, region};

use crate::adapter::{conn, consumer};
use crate::failure;

/// One allocation-free receive bound to a logical consumer lease.
pub struct Recv<'recv, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, M: consumer::Mode> {
    lease: Option<&'recv mut consumer::Lease<'o, 'r, 'c, 'b, 'd, ID, M>>,
    wait: fanout::Registration<'o, 'd>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'recv, 'd: 'b, const ID: u8, M: consumer::Mode>
    Recv<'recv, 'o, 'r, 'c, 'b, 'd, ID, M>
{
    pub(super) const fn new(lease: &'recv mut consumer::Lease<'o, 'r, 'c, 'b, 'd, ID, M>) -> Self {
        Self {
            lease: Some(lease),
            wait: fanout::Registration::new(),
        }
    }

    pub fn poll(
        &mut self,
        mut access: local::Access<'_, region::Identity<'d>>,
        mut next: impl FnMut() -> wake::Waker<'d>,
    ) -> task::Poll<
        Result<
            Option<consumer::Delivery<'o, 'r, 'c, 'b, 'd, ID, M>>,
            failure::Runtime<'c, 'b, 'd, ID>,
        >,
    > {
        loop {
            let Some(lease) = self.lease.as_deref_mut() else {
                return task::Poll::Ready(Err(failure::Runtime::from(failure::Error::Polled)));
            };
            let Some(key) = lease.key.occupied() else {
                self.lease = None;
                return task::Poll::Ready(Ok(None));
            };
            let Some(ready) = lease.ready else {
                let watch = conn::Observe {
                    core: lease.root.core.project(),
                };
                match watch.poll_ready(access.reborrow(), &mut self.wait, next()) {
                    task::Poll::Pending => return task::Poll::Pending,
                    task::Poll::Ready(Ok(ready)) => {
                        lease.ready = Some(ready);
                        continue;
                    }
                    task::Poll::Ready(Err(error)) => {
                        self.lease = None;
                        return task::Poll::Ready(Err(error));
                    }
                }
            };
            match lease
                .root
                .poll(access.reborrow(), ready, lease.channel, key.get(), next())
            {
                Ok(task::Poll::Pending) => return task::Poll::Pending,
                Ok(task::Poll::Ready(item)) => {
                    if item.is_none() {
                        let (key, _) = key.take();
                        key.into_inner().complete();
                    }
                    self.lease = None;
                    return task::Poll::Ready(Ok(item));
                }
                Err(error) => {
                    lease.ready = None;
                    let watch = conn::Observe {
                        core: lease.root.core.project(),
                    };
                    match watch.poll_ready(access.reborrow(), &mut self.wait, next()) {
                        task::Poll::Pending => return task::Poll::Pending,
                        task::Poll::Ready(Ok(live)) => {
                            if live != ready {
                                lease.ready = Some(live);
                            } else {
                                self.lease = None;
                                return task::Poll::Ready(Err(error));
                            }
                        }
                        task::Poll::Ready(Err(error)) => {
                            self.lease = None;
                            return task::Poll::Ready(Err(error));
                        }
                    }
                }
            }
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Option<&mut consumer::Lease<'static, 'static, 'static, 'static, 'static, 0>>>(
        ) == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<
            Recv<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                0,
                crate::message::ack::Manual,
            >,
        >() == 3 * mem::size_of::<usize>()
    );
};
