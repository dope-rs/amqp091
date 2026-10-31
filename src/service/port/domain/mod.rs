use data::compose;
use dope::core::driver::route::KeyTag;
use dope::manifold::client::{close, port};
use dope::net::link::egress::data;
use o3::base::cell::{local, region};
use o3::buffer::{pool, resident};

use crate::failure::{recovery, terminal, wire};
use crate::protocol::wire::Writer;
use crate::protocol::wire::text::resident::Pool;
use crate::service::{self, content, runtime};
use crate::state::conn::recovery::Plan;
use crate::state::{epoch, shared};
use crate::{config, failure};

mod admits;
mod control;
pub(super) mod egress;
mod settings;
mod storage;
mod waiters;

pub(crate) use admits::Admission;
pub(crate) use control::Control;
pub(in crate::service) use settings::Settings;
pub(in crate::service) use storage::Storage;
pub(crate) use waiters::Waiters;

pub(crate) struct Domain<'r, 'c, 'b, 'd, const ID: u8> {
    pub(in crate::service) transport:
        port::Port<'b, 'd, compose::Tagged<pool::Cursor, runtime::Authority<'d, ID>>, KeyTag<ID>>,
    pub(crate) session: shared::Root<'r, 'c, 'b, 'd, ID>,
    pub(in crate::service) coordination: service::Coord<'d, ID>,
    pub(in crate::service) admissions: crate::permits::Registry<'c, 'd>,
    pub(in crate::service::port) egress: egress::Egress,
    pub(crate) content: content::Store<'r, 'c, 'b, 'd, ID>,
    pub(in crate::service) storage: Storage<'c, 'b, 'd>,
    pub(in crate::service) settings: Settings<'d>,
}

pub(crate) enum EpochAccess<'c, 'b, 'd, R> {
    Live(R),
    Terminal(terminal::Outcome<'c, 'b, 'd>),
    Unknown,
}

pub(crate) enum Replay {
    Idle,
    Frame,
    Ready,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Domain<'r, 'c, 'b, 'd, ID> {
    pub(crate) fn encode(
        &self,
        f: impl FnOnce(&mut Writer),
    ) -> Result<pool::Cursor, failure::Error> {
        let frame = self
            .egress
            .regular
            .acquire()
            .ok_or(failure::Error::Capacity(wire::Capacity::OutboundBuffer))?;
        match frame.encode(f) {
            Ok(frame) => Ok(frame.into_output()),
            Err(error) => {
                Admission::new(self).sync();
                Err(error)
            }
        }
    }

    pub(crate) fn replay(
        &self,
        mut access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
    ) -> service::Admit<Replay> {
        let Some(frame) = self.egress.regular.acquire() else {
            return service::Admit::Unavailable;
        };
        let frame = match self.session.write(access.reborrow(), |state| {
            frame.try_encode(|output| {
                let plan = state.recovery.plan(&mut state.routes, |step, ticket| {
                    step.write(output, ticket.lease().id());
                    ticket
                })?;
                if let Plan::Ready(target) = &plan {
                    state.connection.lifecycle.mark_ready(*target)?;
                }
                Ok(plan)
            })
        }) {
            Ok(frame) => frame,
            Err(error) => {
                Admission::new(self).sync();
                return service::Admit::Error(error);
            }
        };
        let (frame, plan) = frame.into_parts();
        let ticket = match plan {
            Plan::Item(ticket) => ticket,
            Plan::Idle => {
                frame.recycle();
                Admission::new(self).sync();
                return service::Admit::Item(Replay::Idle);
            }
            Plan::Ready(_) => {
                frame.recycle();
                Admission::new(self).sync();
                return service::Admit::Item(Replay::Ready);
            }
        };
        Admission::new(self).reserve(
            access,
            target,
            Some(ticket.lease()),
            frame,
            |region, bytes| {
                self.session.write(local::Access::new(region), |state| {
                    state.connection.try_frame(bytes.len())?;
                    state
                        .recovery
                        .commit(ticket, &mut state.routes, self.identities())?;
                    Ok(Replay::Frame)
                })
            },
        )
    }

    pub(in crate::service) const fn causes(&self) -> &'c terminal::Pool<'b, 'd> {
        self.storage.causes
    }

    pub(crate) fn fail(
        &self,
        access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        error: failure::Error,
    ) {
        let stores = self.content.stores();
        let change = self.session.write(access, |state| {
            state.fail(
                target,
                recovery::Fault::terminal(error.into()),
                || self.storage.causes.reserve(),
                stores,
            )
        });
        match change {
            shared::Failure::Ignored => return,
            shared::Failure::Applied(_) => {}
        }
        self.transport
            .close(target.token(), close::Reason::Protocol);
        self.coordination.waiters().wake_all();
        self.admission().wake_all();
    }

    pub(crate) const fn coordination(&self) -> &service::Coord<'d, ID> {
        &self.coordination
    }

    pub(crate) const fn admission(&self) -> &crate::permits::Registry<'c, 'd> {
        &self.admissions
    }

    pub(crate) const fn transport(
        &self,
    ) -> &port::Port<'b, 'd, compose::Tagged<pool::Cursor, runtime::Authority<'d, ID>>, KeyTag<ID>>
    {
        &self.transport
    }

    pub(super) const fn egress(&self) -> &egress::Egress {
        &self.egress
    }

    pub(crate) const fn control(&self) -> Control<'_, 'r, 'c, 'b, 'd, ID> {
        Control::new(self)
    }

    pub(crate) fn config(&self) -> &config::Config {
        &self.settings.config
    }

    pub(crate) const fn budget(&self) -> &resident::Budget<'d> {
        &self.storage.budget
    }

    pub(crate) const fn identities(&self) -> &'b Pool {
        self.storage.identities
    }
}
