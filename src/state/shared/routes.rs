use std::ops;

use dope::core::driver;
use o3::depot::arena::slab;

use crate::failure::{self, terminal, wire};
use crate::protocol::topology::records::queue;
use crate::protocol::topology::{self};
use crate::state::channel::change;
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

    pub(crate) fn generation(&self) -> Option<channels::Generation> {
        self.channels.generation()
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

    pub(crate) fn try_commit(
        &mut self,
        lease: channels::Lease<'d, ID>,
        next: send::Next,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> Result<(), failure::Error> {
        let channel = lease.id();
        let (session, local) = self
            .inner
            .channels
            .try_get_mut(lease)
            .map_err(failure::Error::Protocol)?;
        let effect = session
            .method_state()
            .methods
            .claim(channel, &next)
            .map_err(failure::Error::Protocol)?
            .commit();
        if matches!(effect, send::Effect::Closing) {
            local.clear();
            session.close_sent(stores);
        }
        Ok(())
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

    pub(crate) fn transport_ended(
        &mut self,
        cause: &terminal::Lease<'c, 'b, 'd>,
        stores: channel::Stores<'r, 'c, 'b, 'd>,
    ) -> channel::Freed {
        let mut freed = channel::Freed::Available;
        self.for_each(|chan, topo| {
            topo.disconnected();
            freed = freed.merge(chan.change(topo, stores, change::Event::TransportEnded { cause }));
        });
        freed
    }

    pub(crate) fn next<R>(
        &mut self,
        max: u16,
        init: impl FnOnce(channels::Index<'c, ID>, channels::Lease<'d, ID>) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        let upper = if max == 0 { u16::MAX } else { max };
        self.inner.channels.acquire::<R, false>(upper, init)
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
        self.inner.channels.acquire::<R, true>(id, init)
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
