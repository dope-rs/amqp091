use std::mem;

use o3::base::cell::{local, region};

use crate::adapter::channel::{self, route};
use crate::adapter::method::Call;
use crate::adapter::{conn, core};
use crate::failure;
use crate::protocol::method;
use crate::state::channel::cancel::lanes;
use crate::state::channel::methods::DetachAction;

#[repr(transparent)]
pub struct Methods<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
}

impl<const ID: u8> Copy for Methods<'_, '_, '_, '_, '_, ID> {}

impl<const ID: u8> Clone for Methods<'_, '_, '_, '_, '_, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> Methods<'o, 'r, 'c, 'b, 'd, ID> {
    pub(in crate::adapter) const fn new(
        core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
    ) -> Self {
        Self { core }
    }

    pub fn reserve<M: method::Op<'d>>(
        self,
        mut access: local::Access<'_, region::Identity<'d>>,
        target: conn::Ready<'d, ID>,
        lease: channel::Lease<'c, 'd, ID>,
        method: M,
    ) -> Result<Call<'o, 'r, 'c, 'b, 'd, ID, M>, failure::Runtime<'c, 'b, 'd, ID>> {
        let expect = <M::Response as method::Reply>::KIND;
        let route = route::Route::new(self.core, target, lease);
        let result = route.with(access.reborrow(), |route| {
            let state = route.channel.method_state();
            let active = state.publishing.is_active();
            let reaped = self
                .core
                .cancel()
                .try_reap::<lanes::Method, _>(route.index, lease.raw(), |cancel| {
                    Some(state.methods.reap(state.phase, active, cancel.identity()))
                })?
                .transpose()?;
            let (action, next) = match reaped {
                Some(result) => (Some(result.action), result.control),
                None => (None, None),
            };
            let cancel = if matches!(action, Some(DetachAction::CloseAbandonedOpen)) {
                Err(failure::Error::Closed)
            } else {
                state.methods.enqueue(state.phase, expect, |key| {
                    self.core
                        .cancel()
                        .issue(route.index, lease.raw(), lanes::Slot::from(key), key)
                })
            };
            Ok::<_, failure::Error>((route.index.lane(), cancel, action, next))
        });
        let entry = match result {
            crate::service::EpochAccess::Live(Some(result)) => match result {
                Ok(entry) => entry,
                Err(error) => return Err(failure::Runtime::from(error)),
            },
            crate::service::EpochAccess::Terminal(cause) => {
                return Err(cause.branded());
            }
            crate::service::EpochAccess::Live(None) | crate::service::EpochAccess::Unknown => {
                return Err(failure::Runtime::from(failure::Error::Closed));
            }
        };
        if let Some(next) = entry.3 {
            self.core
                .send(access.reborrow(), route.epoch().raw(), lease.raw(), next);
        }
        self.core.permits().wake_channel(entry.0);
        if let Some(action) = entry.2 {
            self.core.detach(access, lease, action);
        }
        let cancel = entry.1.map_err(failure::Runtime::from)?;
        Ok(Call::new(route, cancel, method))
    }
}

const _: () = assert!(
    mem::size_of::<Methods<'static, 'static, 'static, 'static, 'static, 0>>()
        == mem::size_of::<usize>()
);
