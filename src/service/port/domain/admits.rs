use std::ops;

use data::compose;
use dope::core::driver::schedule::ready::wake;
use dope::manifold::client::port::write::Reserve;
use dope::net::link::egress::{Rejected, Rejection, data};
use o3::base::cell::{local, region};
use o3::buffer::pool;

use crate::protocol::wire::Writer;
use crate::service::port::domain::{Transport, egress};
use crate::service::{self, Domain};
use crate::state::{channels, epoch, shared};
use crate::{failure, permits};

/// Exact borrows required to admit one regular outbound frame.
pub(crate) struct Admission<'a, 'r, 'c, 'b, 'd, const ID: u8> {
    session: &'a shared::Root<'r, 'c, 'b, 'd, ID>,
    transport: &'a Transport<'b, 'd, ID>,
    admissions: &'a permits::Registry<'c, 'd>,
    egress: &'a egress::Egress,
}

enum Reject<D> {
    Error(failure::Error),
    Defer(D),
    Inactive,
}

impl<'a, 'r, 'c, 'b, 'd, const ID: u8> Admission<'a, 'r, 'c, 'b, 'd, ID> {
    pub(crate) const fn new(domain: &'a Domain<'r, 'c, 'b, 'd, ID>) -> Self {
        Self {
            session: &domain.session,
            transport: &domain.transport,
            admissions: &domain.admissions,
            egress: &domain.egress,
        }
    }

    pub(crate) fn admit<R>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: Option<channels::Lease<'d, ID>>,
        encode: impl FnOnce(&mut Writer),
        commit: impl for<'s> FnOnce(
            &'s mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>,
        ) -> Result<R, failure::Error>,
    ) -> service::Admit<R> {
        let session = self.session;
        self.try_frame(
            access,
            target,
            lease,
            |output, _| encode(output),
            |region, bytes, ()| {
                session.write(local::Access::new(region), |state| {
                    state.connection.try_frame(bytes.len())?;
                    commit(state)
                })
            },
        )
        .unwrap_or_else(service::Admit::Error)
    }

    pub(crate) fn admit_if<R, D>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: Option<channels::Lease<'d, ID>>,
        encode: impl FnOnce(&mut Writer),
        decide: impl FnOnce(&mut shared::Edit<'_, 'r, 'c, 'b, 'd, ID>) -> ops::ControlFlow<D, R>,
    ) -> Result<service::Admit<R>, D> {
        let session = self.session;
        match self.try_frame(
            access,
            target,
            lease,
            |output, _| encode(output),
            |region, bytes, ()| {
                session.write(local::Access::new(region), |state| {
                    if state.connection.lifecycle.status().epoch() != Some(target) {
                        return Err(Reject::Inactive);
                    }
                    state
                        .connection
                        .try_frame(bytes.len())
                        .map_err(Reject::Error)?;
                    let output = match decide(state) {
                        ops::ControlFlow::Continue(output) => output,
                        ops::ControlFlow::Break(defer) => {
                            return Err(Reject::Defer(defer));
                        }
                    };
                    Ok(output)
                })
            },
        ) {
            Ok(admit) => Ok(admit),
            Err(Reject::Error(error)) => Ok(service::Admit::Error(error)),
            Err(Reject::Defer(defer)) => Err(defer),
            Err(Reject::Inactive) => Ok(service::Admit::Inactive),
        }
    }

    pub(crate) fn try_frame<P, R, E>(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: Option<channels::Lease<'d, ID>>,
        encode: impl FnOnce(&mut Writer, local::Access<'_, region::Identity<'d>>) -> P,
        commit: impl FnOnce(&mut region::Token<region::Identity<'d>>, &[u8], P) -> Result<R, E>,
    ) -> Result<service::Admit<R>, E> {
        let Some(frame) = self.egress.regular.acquire() else {
            return Ok(service::Admit::Unavailable);
        };
        let frame = match frame.encode(|output| encode(output, access.reborrow())) {
            Ok(frame) => frame,
            Err(error) => {
                self.sync();
                return Ok(service::Admit::Error(error));
            }
        };
        let (frame, proof) = frame.into_parts();
        self.try_reserve(access, target, lease, frame, |region, bytes| {
            commit(region, bytes, proof)
        })
    }

    fn try_reserve<R, E>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: Option<channels::Lease<'d, ID>>,
        frame: pool::Cursor,
        commit: impl FnOnce(&mut region::Token<region::Identity<'d>>, &[u8]) -> Result<R, E>,
    ) -> Result<service::Admit<R>, E> {
        let frame = compose::Tagged::new(frame, lease);
        let result = access.with_region(|region| {
            let hold = self.transport.try_reserve(region, target.token(), frame);
            let hold = match hold {
                Reserve::Item(hold) => hold,
                Reserve::Unknown(_frame) | Reserve::Inactive(_frame) => {
                    return Ok(service::Admit::Inactive);
                }
                Reserve::Rejected(_cause) => return Ok(service::Admit::Unavailable),
                Reserve::Invalid(cause) => {
                    let (_, cause) = Rejected::<_>::from(cause).into_parts();
                    return Ok(service::Admit::Error(failure::Error::Egress(cause)));
                }
                Reserve::Empty(cause) => {
                    let (_, cause) = cause.into_parts();
                    return Ok(service::Admit::Error(failure::Error::Egress(
                        Rejection::Empty(cause),
                    )));
                }
            };
            let permit = match hold.validate() {
                Ok(permit) => permit,
                Err(_frame) => return Ok(service::Admit::Inactive),
            };
            match permit.try_with(|region, frame| commit(region, frame.as_ref())) {
                Ok(output) => Ok(service::Admit::Item(output)),
                Err(rejected) => {
                    let (_, cause) = rejected.into_parts();
                    Err(cause)
                }
            }
        });
        if !matches!(&result, Ok(service::Admit::Item(_))) {
            self.sync();
        }
        result
    }

    pub(super) fn reserve<R>(
        self,
        access: local::Access<'_, region::Identity<'d>>,
        target: epoch::Id<'d, ID>,
        lease: Option<channels::Lease<'d, ID>>,
        frame: pool::Cursor,
        commit: impl FnOnce(
            &mut region::Token<region::Identity<'d>>,
            &[u8],
        ) -> Result<R, failure::Error>,
    ) -> service::Admit<R> {
        self.try_reserve(access, target, lease, frame, commit)
            .unwrap_or_else(service::Admit::Error)
    }

    pub(crate) fn sync(self) {
        for _ in self.egress.regular.released() {
            match self.admissions.wake_credit() {
                wake::Once::Woke => {}
                wake::Once::Empty => break,
            }
        }
    }
}
