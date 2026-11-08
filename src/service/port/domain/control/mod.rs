use o3::base::cell::{local, region};

use crate::adapter::channel;
use crate::failure;
use crate::protocol::wire::text::resident;
use crate::service::Domain;
use crate::service::port::domain::control::plan::Plan;
use crate::service::port::domain::control::write::Admission;
use crate::state::channel::{Freed, Phase, Stores, consumer, methods};
use crate::state::channels::Lease;
use crate::state::epoch;

pub(super) mod frame;
mod plan;
pub(crate) mod write;

/// One control-path turn over an address-stable route domain.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct Control<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    domain: &'a Domain<'r, 'c, 'b, 'd, ID>,
}

#[derive(Clone, Copy)]
enum Scope {
    Ready,
    Replay,
}

impl<'a, 'r, 'c, 'b, 'd, const ID: u8> Control<'a, 'r, 'c, 'b, 'd, ID> {
    pub(super) const fn new(domain: &'a Domain<'r, 'c, 'b, 'd, ID>) -> Self {
        Self { domain }
    }

    pub(crate) const fn write(
        self,
        target: epoch::Id<'d, ID>,
        lease: Lease<'d, ID>,
    ) -> write::Write<'a, 'r, 'c, 'b, 'd, ID> {
        write::Write::new(self.domain, target, lease)
    }

    pub(crate) fn retire(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        lease: channel::Lease<'c, 'd, ID>,
        key: consumer::Key,
    ) {
        let stores = self.domain.content.stores();
        let chan = lease.id();
        let target = self.domain.session.read(access.read(), |state| {
            state.connection.lifecycle.status().epoch()
        });
        let Some(target) = target else {
            let _ = self.detach(access.reborrow(), lease.raw(), key, stores);
            self.domain.session.write(access, |state| {
                state.routes.collect(lease.raw());
            });
            return;
        };
        let admit = self
            .write(target, lease.raw())
            .admit(access.reborrow(), |state, egress| {
                match state.routes.project(lease.raw(), |view| {
                    if view.channel.protocol().phase != Phase::Open {
                        return Plan::Missing(Freed::Available);
                    }
                    let mut method = view.channel.method_state();
                    let busy = method.publishing.is_active();
                    let Some(freed) = method.retire(key, stores) else {
                        return Plan::Missing(Freed::Available);
                    };
                    let Some(next) =
                        method
                            .methods
                            .retire(key, *method.phase == Phase::Closing, busy)
                    else {
                        return Plan::Missing(freed);
                    };
                    let Some(next) = next else {
                        return Plan::Pending(None, freed);
                    };
                    Plan::encode(&mut method, egress, chan, next, freed)
                }) {
                    Some(plan) => plan,
                    None => Plan::Missing(Freed::Available),
                }
            });
        let freed = match admit {
            Admission::Missing(freed) => {
                freed.merge(self.detach(access.reborrow(), lease.raw(), key, stores))
            }
            Admission::Continue(freed) | Admission::Pending(freed) => freed,
            Admission::Failed(cause) => {
                self.domain.session.write(access.reborrow(), |state| {
                    state.routes.collect(lease.raw());
                });
                self.domain.fail(access, target, cause);
                return;
            }
        };
        if let Err(error) = self.wake(target, freed) {
            self.domain.fail(access.reborrow(), target, error);
        }
        self.domain.session.write(access, |state| {
            state.routes.collect(lease.raw());
        });
    }

    pub(crate) fn cancel(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        chan: u16,
        cancel: consumer::Cancel<'b, 'd>,
    ) {
        self.cancel_in(access, chan, cancel, Scope::Ready);
    }

    pub(crate) fn replay(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        chan: u16,
        tag: resident::Short<'b, 'd>,
    ) {
        self.cancel_in(access, chan, consumer::Cancel::Orphan(tag), Scope::Replay);
    }

    fn cancel_in(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        chan: u16,
        cancel: consumer::Cancel<'b, 'd>,
        scope: Scope,
    ) {
        let stores = self.domain.content.stores();
        let key = match &cancel {
            consumer::Cancel::Tracked(key) => Some(*key),
            consumer::Cancel::Orphan(_) => None,
        };
        let Some((target, lease)) = self.domain.session.read(access.read(), |state| {
            let target = match scope {
                Scope::Ready => state.connection.lifecycle.status().ready(),
                Scope::Replay => state.recovery.target(),
            }?;
            Some((target, state.routes.lease(chan)?))
        }) else {
            if let Some(key) = key {
                let _ = self.domain.session.write(access, |state| {
                    state
                        .routes
                        .with_id(chan, |view| view.channel.method_state().detach(key, stores))
                });
            }
            return;
        };
        let admit = self
            .write(target, lease)
            .admit(access.reborrow(), |state, egress| {
                match state.routes.project(lease, |view| {
                    if view.channel.protocol().phase != Phase::Open {
                        return Plan::Missing(Freed::Available);
                    }
                    let mut method = view.channel.method_state();
                    let busy = method.publishing.is_active();
                    let next = match cancel {
                        consumer::Cancel::Tracked(key) => {
                            let Some(vacant) = method.methods.vacant_user(key) else {
                                return Plan::Missing(Freed::Available);
                            };
                            vacant.abandon(key, busy)
                        }
                        consumer::Cancel::Orphan(tag) => {
                            let Some(vacant) = method.methods.vacant() else {
                                return Plan::Missing(Freed::Available);
                            };
                            match scope {
                                Scope::Ready => vacant.orphan(tag, busy),
                                Scope::Replay => vacant.retry(tag, busy),
                            }
                        }
                    };
                    let Some(next) = next else {
                        return Plan::Pending(None, Freed::Available);
                    };
                    Plan::encode(&mut method, egress, chan, next, Freed::Available)
                }) {
                    Some(plan) => plan,
                    None => Plan::Missing(Freed::Available),
                }
            });
        let freed = match admit {
            Admission::Missing(freed) => match key {
                Some(key) => freed.merge(self.detach(access.reborrow(), lease, key, stores)),
                None => freed,
            },
            Admission::Continue(freed) | Admission::Pending(freed) => freed,
            Admission::Failed(cause) => {
                self.domain.fail(access, target, cause);
                return;
            }
        };
        if let Err(error) = self.wake(target, freed) {
            self.domain.fail(access, target, error);
        }
    }

    fn detach(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        lease: Lease<'d, ID>,
        key: consumer::Key,
        stores: Stores<'r, 'c, 'b, 'd>,
    ) -> Freed {
        self.domain.session.write(access, |state| {
            state
                .routes
                .project(lease, |view| {
                    view.channel.method_state().detach(key, stores)
                })
                .unwrap_or(Freed::Available)
        })
    }

    pub(crate) fn close(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        lease: channel::Lease<'c, 'd, ID>,
    ) {
        let stores = self.domain.content.stores();
        let chan = lease.id();
        let target = self.domain.session.read(access.read(), |state| {
            state.connection.lifecycle.status().ready()
        });
        let Some(target) = target else {
            let _ = self.domain.session.write(access.reborrow(), |state| {
                state.routes.abandon(lease.raw(), stores)
            });
            return;
        };
        let admit = self
            .write(target, lease.raw())
            .admit(access.reborrow(), |state, egress| {
                match state.routes.project(lease.raw(), |view| {
                    let mut method = view.channel.method_state();
                    if *method.phase != Phase::Open {
                        return Plan::Missing(Freed::Available);
                    }
                    if !matches!(method.methods.head(), methods::Head::Empty) {
                        return Plan::Missing(Freed::Available);
                    }
                    let Some(vacant) = method.methods.vacant() else {
                        return Plan::Missing(Freed::Available);
                    };
                    let next = vacant.close();
                    let plan = if method.publishing.is_active() {
                        Plan::Pending(None, Freed::Available)
                    } else {
                        Plan::encode(&mut method, egress, chan, next, Freed::Available)
                    };
                    view.channel.begin_close(self.domain.coordination.waiters());
                    plan
                }) {
                    Some(plan) => plan,
                    None => Plan::Missing(Freed::Available),
                }
            });
        let freed = match admit {
            Admission::Missing(freed) => {
                freed.merge(self.domain.session.write(access.reborrow(), |state| {
                    state.routes.abandon(lease.raw(), stores)
                }))
            }
            Admission::Continue(freed) | Admission::Pending(freed) => freed,
            Admission::Failed(cause) => {
                self.domain.fail(access, target, cause);
                return;
            }
        };
        if let Err(error) = self.wake(target, freed) {
            self.domain.fail(access, target, error);
        }
    }

    fn wake(self, target: epoch::Id<'d, ID>, freed: Freed) -> Result<(), failure::Error> {
        self.domain.content.wake_route(target, freed)
    }
}
