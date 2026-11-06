use std::num;

use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue;
use dope::manifold::client::close;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::channel::route;
use crate::adapter::publish::send::method::{Kind, Mode, Tickets};
use crate::adapter::{core, step};
use crate::protocol::wire::Writer;
use crate::service::cancel;
use crate::state::channel::cancel::lanes;
use crate::state::channel::methods::send;
use crate::state::conn::status::Retirement;
use crate::{failure, service};

pub(super) struct Stream<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const CONFIRM: bool, const ID: u8>
where
    Kind<CONFIRM>: Mode<'o, 'd, ID>,
{
    route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
    publish: cancel::Ticket<'o, 'd, lanes::Publish, ID>,
    confirm: <Kind<CONFIRM> as Mode<'o, 'd, ID>>::Ticket,
    max: num::NonZero<u32>,
}

pub(super) struct Last<'c> {
    wake: fifo::Index<'c>,
    next: Option<send::Next>,
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const CONFIRM: bool, const ID: u8>
    Stream<'o, 'r, 'c, 'b, 'd, CONFIRM, ID>
where
    Kind<CONFIRM>: Mode<'o, 'd, ID>,
{
    pub(super) fn new(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish>,
        (publish, confirm): Tickets<'o, 'd, <Kind<CONFIRM> as Mode<'o, 'd, ID>>::Ticket, ID>,
        max: num::NonZero<u32>,
    ) -> Self {
        Self {
            route,
            publish,
            confirm,
            max,
        }
    }

    pub(super) const fn route(&self) -> route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Publish> {
        self.route
    }

    pub(super) const fn max(&self) -> num::NonZero<u32> {
        self.max
    }

    pub(super) fn poll_more(
        &self,
        mut access: local::Access<'_, region::Identity<'d>>,
        encode: impl FnOnce(&mut Writer),
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
        wake: wake::Waker<'d>,
    ) -> step::Step<'c, 'b, 'd, (), ID> {
        let route = self.route;
        let key = self.publish.identity();
        let admit = route.admit(access.reborrow(), encode, |route| {
            if route.channel.publish().publishing.is_owner(key) {
                Ok(())
            } else {
                Err(failure::Error::Closed)
            }
        });
        step::Step::admit(route, access, admit, credit, wake)
    }

    pub(super) fn poll_last(
        &self,
        mut access: local::Access<'_, region::Identity<'d>>,
        encode: impl FnOnce(&mut Writer),
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
        wake: wake::Waker<'d>,
    ) -> step::Step<'c, 'b, 'd, Last<'c>, ID> {
        let route = self.route;
        let key = self.publish.identity();
        let admit = route.admit(access.reborrow(), encode, |route| {
            let state = route.channel.publish();
            let active = state
                .publishing
                .get(key)
                .ok_or_else(|| failure::Error::Closed)?;
            let next = state.methods.next::<false>().1;
            active.finish();
            Ok(Last {
                wake: route.index.lane(),
                next,
            })
        });
        step::Step::admit(route, access, admit, credit, wake)
    }

    pub(super) fn finish(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        last: Last<'c>,
    ) -> <Kind<CONFIRM> as Mode<'o, 'd, ID>>::Ticket {
        let core = self.route.core();
        self.publish.complete();
        if let Some(next) = last.next {
            core.send(
                access.reborrow(),
                self.route.epoch().raw(),
                self.route.lease().raw(),
                next,
            );
        }
        core.permits().wake_channel(last.wake);
        self.confirm
    }

    pub(super) fn cancel(&self) {
        <Kind<CONFIRM> as Mode<'o, 'd, ID>>::cancel(&self.publish, &self.confirm);
    }

    pub(super) fn abort(&self, access: local::Access<'_, region::Identity<'d>>) {
        let core = self.route.core();
        let key = self.publish.identity();
        let close = matches!(
            self.route.with(access, |route| {
                match route.channel.publish().publishing.get(key) {
                    Some(active) => {
                        active.finish();
                        Some(())
                    }
                    None => None,
                }
            }),
            service::EpochAccess::Live(Some(Some(())))
        );
        self.cancel();
        if close {
            core.close(
                self.route.epoch().raw(),
                Retirement::Permanent.abort(close::Reason::Protocol),
            );
            core.waiters().wake_all();
        }
    }
}
