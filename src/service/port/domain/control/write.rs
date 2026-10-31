use std::mem;

use dope::manifold::client::port;
use dope::net::link::egress::{Rejected, Rejection};
use o3::base::cell::{local, region};
use port::write::Reserve;
use runtime::Authority;

use crate::failure;
use crate::service::port::domain::control::plan::Plan;
use crate::service::port::domain::egress;
use crate::service::{Domain, runtime};
use crate::state::channel::methods::send;
use crate::state::channels::Lease;
use crate::state::{channel, epoch, shared};

#[must_use = "write admission owns any route freed while preparing it"]
pub(super) enum Admission {
    Continue(channel::Freed),
    Missing(channel::Freed),
    Failed(failure::Error),
}

const _: () = {
    const FAILURE: usize = mem::size_of::<failure::Error>();
    assert!(mem::size_of::<Admission>() <= FAILURE + mem::size_of::<usize>());
};

pub(crate) struct Write<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    domain: &'a Domain<'r, 'c, 'b, 'd, ID>,
    target: epoch::Id<'d, ID>,
    lease: Lease<'d, ID>,
}

impl<'a, 'r, 'c, 'b, 'd, const ID: u8> Write<'a, 'r, 'c, 'b, 'd, ID> {
    pub(super) const fn new(
        domain: &'a Domain<'r, 'c, 'b, 'd, ID>,
        target: epoch::Id<'d, ID>,
        lease: Lease<'d, ID>,
    ) -> Self {
        Self {
            domain,
            target,
            lease,
        }
    }

    pub(super) fn admit(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        prepare: impl for<'s> FnOnce(
            &'s mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>,
            &egress::Egress,
        ) -> Plan<send::Next>,
    ) -> Admission {
        let plan = self.domain.session.write(access.reborrow(), |state| {
            prepare(state, self.domain.egress())
        });
        let (frame, next, freed) = match plan {
            Plan::Send(frame, next, freed) => (frame, next, freed),
            Plan::Pending(freed) => return Admission::Continue(freed),
            Plan::Missing(freed) => return Admission::Missing(freed),
            Plan::Failed(cause) => return Admission::Failed(cause),
        };
        let frame = Authority::tagged(frame, Some(self.lease));
        access.with_region(|region| {
            let slot = self
                .domain
                .transport
                .try_reserve(region, self.target.token(), frame);
            let slot = match slot {
                Reserve::Item(slot) => slot,
                Reserve::Unknown(frame) | Reserve::Inactive(frame) => {
                    let (frame, _) = frame.into_parts();
                    frame.recycle();
                    return Admission::Missing(freed);
                }
                Reserve::Rejected(cause) => {
                    let (frame, _) = cause.load.into_inner().into_parts();
                    frame.recycle();
                    return Admission::Continue(freed);
                }
                Reserve::Invalid(cause) => {
                    let (load, cause) = Rejected::<_>::from(cause).into_parts();
                    let (frame, _) = load.into_inner().into_parts();
                    frame.recycle();
                    return Admission::Failed(failure::Error::Egress(cause));
                }
                Reserve::Empty(cause) => {
                    let (load, cause) = cause.into_parts();
                    let (frame, _) = load.into_inner().into_parts();
                    frame.recycle();
                    return Admission::Failed(failure::Error::Egress(Rejection::Empty(cause)));
                }
            };
            let slot = match slot.validate() {
                Ok(slot) => slot,
                Err(frame) => {
                    let (frame, _) = frame.into_inner().into_parts();
                    frame.recycle();
                    return Admission::Continue(freed);
                }
            };
            let effect = slot.try_commit(|region| {
                self.domain
                    .session
                    .write(local::Access::new(region), |state| {
                        let prepared = state.routes.try_prepare(self.lease, next)?;
                        Ok(prepared.commit())
                    })
            });
            match effect {
                Ok(send::Effect::Sent | send::Effect::Closing) => Admission::Continue(freed),
                Err(rejected) => {
                    let (frame, cause) = rejected.into_parts();
                    let (frame, _) = frame.into_inner().into_parts();
                    frame.recycle();
                    Admission::Failed(cause)
                }
            }
        })
    }

    pub(crate) fn send(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        next: send::Next,
    ) -> Result<(), failure::Error> {
        let lease = self.lease;
        match self.admit(access, |state, egress| {
            state
                .routes
                .project(lease, |view| {
                    Plan::encode(
                        &mut view.channel.method_state(),
                        egress,
                        lease.id(),
                        next,
                        channel::Freed::Available,
                    )
                })
                .unwrap_or(Plan::Missing(channel::Freed::Available))
        }) {
            Admission::Failed(cause) => Err(cause),
            Admission::Continue(_) | Admission::Missing(_) => Ok(()),
        }
    }
}
