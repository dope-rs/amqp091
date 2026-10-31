use std::ops;

use dope::core::driver;
use o3::depot::arena::slab;

use crate::failure::{self, wire};
use crate::protocol::topology::records::queue;
use crate::protocol::topology::{self};
use crate::state::channel::methods::send;
use crate::state::{channel, channels};

pub(crate) struct Routes<'r, 'c, 'b, 'd, const ID: u8> {
    channels: channels::Table<'r, 'c, 'b, 'd, ID>,
    topology: topology::Table<'c, 'd, ID>,
}

pub(crate) struct Edit<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    inner: &'a mut Routes<'r, 'c, 'b, 'd, ID>,
    values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
}

pub(crate) struct View<'a, 's, 'v, 'r, 'c, 'b, 'd, const ID: u8> {
    pub(crate) index: channels::Index<'c, ID>,
    pub(crate) channel: &'a mut channel::Session<'r, 'c, 'b, 'd, ID>,
    pub(crate) topology: &'s mut topology::State<'v, 'c, 'b, 'd, ID>,
}

/// Exclusive route proof for an encoded control frame.
#[must_use = "the prepared route must be committed with its frame"]
pub(crate) struct Prepared<'a, 'b, 'd, const ID: u8> {
    send: send::Prepared<'a, 'b, 'd>,
    local: &'a mut topology::Local<'d, ID>,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Routes<'r, 'c, 'b, 'd, ID> {
    pub(crate) fn try_new(
        plan: channels::Plan,
        driver: driver::Reference<'d>,
        permits: &crate::permits::Registry<'c, 'd>,
    ) -> Result<Self, channels::Error> {
        Ok(Self {
            channels: channels::Table::try_new(plan, driver, permits)?,
            topology: topology::Table::try_new(plan.topology(), permits)?,
        })
    }

    pub(crate) fn edit<'a>(
        &'a mut self,
        values: &'c slab::Cell<queue::Declare<'b, 'd>, queue::Tag, { u32::MAX }, false>,
    ) -> Edit<'a, 'r, 'c, 'b, 'd, ID> {
        Edit {
            inner: self,
            values,
        }
    }

    pub(crate) fn lease(&self, id: u16) -> Option<channels::Lease<'d, ID>> {
        self.channels.lease(id)
    }

    pub(crate) fn next_tracked(
        &self,
        after: Option<channels::Index<'c, ID>>,
    ) -> Option<(channels::Index<'c, ID>, channels::Lease<'d, ID>)> {
        self.channels.next_tracked(after)
    }

    pub(crate) fn len(&self) -> usize {
        self.channels.tracked_len()
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Edit<'_, 'r, 'c, 'b, 'd, ID> {
    pub(crate) fn with_id<R>(
        &mut self,
        id: u16,
        f: impl for<'a, 's, 'v> FnOnce(View<'a, 's, 'v, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> Option<R> {
        let lease = self.inner.channels.lease(id)?;
        self.project(lease, f)
    }

    pub(crate) fn project<R>(
        &mut self,
        lease: channels::Lease<'d, ID>,
        f: impl for<'a, 's, 'v> FnOnce(View<'a, 's, 'v, 'r, 'c, 'b, 'd, ID>) -> R,
    ) -> Option<R> {
        self.inner.channels.edit(
            &mut self.inner.topology,
            self.values,
            lease,
            |index, chan, topo| {
                f(View {
                    index,
                    channel: chan,
                    topology: topo,
                })
            },
        )
    }

    pub(crate) fn try_project<R>(
        &mut self,
        lease: channels::Lease<'d, ID>,
        f: impl for<'a, 's, 'v> FnOnce(
            &'a mut channel::Session<'r, 'c, 'b, 'd, ID>,
            &'s mut topology::State<'v, 'c, 'b, 'd, ID>,
        ) -> R,
    ) -> Result<R, failure::Error> {
        let (channel, local) = self
            .inner
            .channels
            .try_get_mut(lease)
            .map_err(failure::Error::Protocol)?;
        let mut topology = topology::State::new(local, &mut self.inner.topology, self.values);
        Ok(f(channel, &mut topology))
    }

    pub(crate) fn try_prepare(
        &mut self,
        lease: channels::Lease<'d, ID>,
        next: send::Next,
    ) -> Result<Prepared<'_, 'b, 'd, ID>, failure::Error> {
        let channel = lease.id();
        let (session, local) = self
            .inner
            .channels
            .try_get_mut(lease)
            .map_err(failure::Error::Protocol)?;
        let send = session
            .method_state()
            .try_prepare(channel, next)
            .map_err(failure::Error::Protocol)?;
        Ok(Prepared { send, local })
    }

    pub(crate) fn for_each(
        &mut self,
        f: impl for<'a, 's, 'v> FnMut(
            &'a mut channel::Session<'r, 'c, 'b, 'd, ID>,
            &'s mut topology::State<'v, 'c, 'b, 'd, ID>,
        ),
    ) {
        self.inner
            .channels
            .visit(&mut self.inner.topology, self.values, f);
    }

    pub(crate) fn next<R>(
        &mut self,
        max: u16,
        init: impl FnOnce(channels::Index<'c, ID>, channels::Lease<'d, ID>) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        let upper = if max == 0 { u16::MAX } else { max };
        self.inner.channels.acquire_next(upper, init)
    }

    pub(crate) fn abandon(
        &mut self,
        lease: channels::Lease<'d, ID>,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> channel::Freed {
        let freed =
            self.inner
                .channels
                .abandon(&mut self.inner.topology, self.values, lease, stores);
        self.collect(lease);
        freed
    }

    pub(crate) fn collect(&mut self, lease: channels::Lease<'d, ID>) {
        self.inner.channels.collect(lease);
    }

    pub(crate) fn sweep(&mut self) {
        let mut after = None;
        loop {
            let Some((index, lease)) = self.inner.channels.releasable(after) else {
                return;
            };
            after = Some(index);
            self.collect(lease);
        }
    }

    pub(crate) fn reserve<R>(
        &mut self,
        id: u16,
        max: u16,
        init: impl FnOnce(channels::Index<'c, ID>, channels::Lease<'d, ID>) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        if id == 0 {
            return Err(failure::Error::Protocol(wire::Fault::ChannelZero));
        }
        if max != 0 && id > max {
            return Err(failure::Error::Protocol(wire::Fault::ChannelMax {
                channel: id,
                max,
            }));
        }
        self.inner.channels.acquire(id, init)
    }
}

impl<const ID: u8> Prepared<'_, '_, '_, ID> {
    pub(crate) fn commit(self) -> send::Effect {
        let effect = self.send.commit();
        if matches!(&effect, send::Effect::Closing) {
            self.local.clear();
        }
        effect
    }
}

const _: () = assert!(
    std::mem::size_of::<View<'_, '_, '_, '_, '_, '_, '_, 0>>()
        == std::mem::size_of::<(
            channels::Index<'_, 0>,
            &mut channel::Session<'_, '_, '_, '_, 0>,
            &mut topology::State<'_, '_, '_, '_, 0>,
        )>()
);
const _: () = assert!(
    std::mem::size_of::<Prepared<'_, '_, '_, 0>>()
        == std::mem::size_of::<(&mut (), &mut (), &mut ())>()
);
const _: () = assert!(
    std::mem::align_of::<View<'_, '_, '_, '_, '_, '_, '_, 0>>()
        == std::mem::align_of::<(
            channels::Index<'_, 0>,
            &mut channel::Session<'_, '_, '_, '_, 0>,
            &mut topology::State<'_, '_, '_, '_, 0>,
        )>()
);

impl<'r, 'c, 'b, 'd, const ID: u8> ops::Deref for Edit<'_, 'r, 'c, 'b, 'd, ID> {
    type Target = Routes<'r, 'c, 'b, 'd, ID>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}
