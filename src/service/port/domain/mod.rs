use data::compose;
use dope::core::driver::route::KeyTag;
use dope::manifold::client::{close, port};
use dope::net::link::egress::data;
use o3::base::cell::{local, region};
use o3::buffer::{pool, resident};

use crate::failure::{recovery, terminal, wire};
use crate::protocol::wire::Writer;
use crate::protocol::wire::text::resident::Pool;
use crate::service::{self, content};
use crate::state::conn::recovery::Plan;
use crate::state::{channels, epoch, shared};
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

type Transport<'b, 'd, const ID: u8> =
    port::Port<'b, 'd, compose::Tagged<pool::Cursor, Option<channels::Lease<'d, ID>>>, KeyTag<ID>>;

pub(crate) struct Domain<'r, 'c, 'b, 'd, const ID: u8> {
    pub(in crate::service) transport: Transport<'b, 'd, ID>,
    pub(crate) session: shared::Root<'r, 'c, 'b, 'd, ID>,
    pub(in crate::service) coordination: service::Coord<'d, ID>,
    pub(in crate::service) admissions: crate::permits::Registry<'c, 'd>,
    pub(in crate::service::port) egress: egress::Egress,
    pub(crate) content: content::Store<'r, 'c, 'b, 'd, ID>,
    pub(in crate::service) storage: Storage<'c, 'b, 'd>,
    pub(in crate::service) settings: Settings<'d>,
}

const _: () = assert!(
    std::mem::size_of::<compose::Tagged<pool::Cursor, Option<channels::Lease<'static, 0>>>>()
        == std::mem::size_of::<pool::Cursor>() + std::mem::size_of::<usize>()
);

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
    pub(crate) fn flow(
        &self,
        access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: channels::Lease<'d, ID>,
        active: bool,
    ) -> Result<(), failure::Error> {
        use crate::protocol::method::{Encode, channel::FlowOk};
        use port::error::Enqueue;

        let frame = self
            .egress
            .control()
            .ok_or_else(|| failure::Error::Capacity(wire::Capacity::OutboundBuffer))?
            .encode(|output| FlowOk { active }.write(output, lease.id()))?
            .into_output();
        let frame = compose::Tagged::new(frame, Some(lease));
        access
            .with_region(|region| self.transport.try_enqueue(region, target.token(), frame))
            .map_err(|error| match error {
                Enqueue::Unknown(_) | Enqueue::Inactive(_) => failure::Error::Closed,
                Enqueue::Rejected(rejected) => {
                    let (_, cause) = rejected.into_parts();
                    failure::Error::Egress(cause)
                }
            })
    }

    pub(crate) fn encode(
        &self,
        f: impl FnOnce(&mut Writer),
    ) -> Result<pool::Cursor, failure::Error> {
        let frame = self
            .egress
            .regular
            .acquire()
            .ok_or_else(|| failure::Error::Capacity(wire::Capacity::OutboundBuffer))?;
        match frame.encode(f) {
            Ok(frame) => Ok(frame.into_output()),
            Err(error) => {
                Admission::new(self).sync();
                Err(error)
            }
        }
    }

    pub(in crate::service) fn replay_available(&self) -> bool {
        self.egress.regular.is_available()
    }

    pub(crate) fn replay(
        &self,
        mut access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
    ) -> service::Admit<Replay> {
        let plan = match self.session.write(access.reborrow(), |state| {
            let ready = state.connection.input.is_ready();
            let plan = state.recovery.plan(&mut state.routes, |step, ticket| {
                if !ready {
                    return service::Admit::Inactive;
                }
                let Some(frame) = self.egress.regular.acquire() else {
                    return service::Admit::Unavailable;
                };
                match frame.encode(|output| {
                    step.write(output, ticket.lease().id());
                    ticket
                }) {
                    Ok(frame) => service::Admit::Item(frame.into_parts()),
                    Err(error) => service::Admit::Error(error),
                }
            })?;
            if let Plan::Ready(target) = &plan {
                state.connection.lifecycle.mark_ready(*target)?;
            }
            Ok::<_, failure::Error>(plan)
        }) {
            Ok(plan) => plan,
            Err(error) => {
                Admission::new(self).sync();
                return service::Admit::Error(error);
            }
        };
        let (frame, ticket) = match plan {
            Plan::Item(service::Admit::Item(frame)) => frame,
            Plan::Idle => {
                return service::Admit::Item(Replay::Idle);
            }
            Plan::Ready(_) => {
                return service::Admit::Item(Replay::Ready);
            }
            Plan::Item(service::Admit::Unavailable) => return service::Admit::Unavailable,
            Plan::Item(service::Admit::Inactive) => return service::Admit::Inactive,
            Plan::Item(service::Admit::Error(error)) => {
                Admission::new(self).sync();
                return service::Admit::Error(error);
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
        let retire = match change {
            shared::Failure::Ignored => return,
            shared::Failure::Applied(retire) => retire,
        };
        self.transport
            .close(target.token(), retire.abort(close::Reason::Protocol));
        self.wake_all();
    }

    pub(crate) fn wake_all(&self) {
        self.coordination.waiters().wake_all();
        self.admission().wake_all();
    }

    pub(crate) const fn coordination(&self) -> &service::Coord<'d, ID> {
        &self.coordination
    }

    pub(crate) const fn admission(&self) -> &crate::permits::Registry<'c, 'd> {
        &self.admissions
    }

    pub(crate) const fn transport(&self) -> &Transport<'b, 'd, ID> {
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
