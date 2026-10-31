use dope::core::driver::schedule::ready::wake;
use dope::core::driver::schedule::ready::wake::queue;
use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::channel::route;
use crate::adapter::core;
use crate::{failure, service};

pub(in crate::adapter) trait Source<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>:
    Copy
{
    type Kind: core::Capability
        + core::Allows<core::State>
        + core::Allows<core::Egress>
        + core::Allows<core::Registry>;

    fn core(self) -> core::Core<'o, 'r, 'c, 'b, 'd, ID, Self::Kind>;

    fn live(
        self,
        cx: local::Access<'_, region::Identity<'d>>,
    ) -> Result<(), failure::Runtime<'c, 'b, 'd, ID>>;
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K> Source<'o, 'r, 'c, 'b, 'd, ID>
    for core::Gate<'o, 'r, 'c, 'b, 'd, ID, K>
where
    K: core::Capability
        + core::Allows<core::State>
        + core::Allows<core::Egress>
        + core::Allows<core::Registry>,
{
    type Kind = K;

    fn core(self) -> core::Core<'o, 'r, 'c, 'b, 'd, ID, K> {
        self.core
    }

    fn live(
        self,
        cx: local::Access<'_, region::Identity<'d>>,
    ) -> Result<(), failure::Runtime<'c, 'b, 'd, ID>> {
        match self.status(cx) {
            service::EpochAccess::Live(()) => Ok(()),
            service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            service::EpochAccess::Unknown => Err(failure::Runtime::from(failure::Error::Closed)),
        }
    }
}

impl<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8, K> Source<'o, 'r, 'c, 'b, 'd, ID>
    for route::Route<'o, 'r, 'c, 'b, 'd, ID, K>
where
    K: core::Capability
        + core::Allows<core::State>
        + core::Allows<core::Egress>
        + core::Allows<core::Registry>,
{
    type Kind = K;

    fn core(self) -> core::Core<'o, 'r, 'c, 'b, 'd, ID, K> {
        self.gate.core
    }

    fn live(
        self,
        cx: local::Access<'_, region::Identity<'d>>,
    ) -> Result<(), failure::Runtime<'c, 'b, 'd, ID>> {
        match self.gate.with(cx, |state| {
            state.routes.lease(self.lease.id()) == Some(self.lease.raw())
        }) {
            service::EpochAccess::Live(true) => Ok(()),
            service::EpochAccess::Terminal(cause) => Err(cause.branded()),
            service::EpochAccess::Live(false) | service::EpochAccess::Unknown => {
                Err(failure::Runtime::from(failure::Error::Closed))
            }
        }
    }
}

pub(in crate::adapter) enum Step<'c, 'b: 'c, 'd: 'b, T, const ID: u8> {
    Pending,
    Ready(T, wake::Waker<'d>),
    Error(failure::Runtime<'c, 'b, 'd, ID>),
}

impl<'c, 'b: 'c, 'd: 'b, T, const ID: u8> Step<'c, 'b, 'd, T, ID> {
    pub(in crate::adapter) fn map<U>(self, map: impl FnOnce(T) -> U) -> Step<'c, 'b, 'd, U, ID> {
        match self {
            Self::Pending => Step::Pending,
            Self::Ready(value, wake) => Step::Ready(map(value), wake),
            Self::Error(error) => Step::Error(error),
        }
    }

    pub(in crate::adapter) fn admit<'o, 'r: 'o, S>(
        source: S,
        cx: local::Access<'_, region::Identity<'d>>,
        admit: core::Admit<'c, 'b, 'd, T, ID>,
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
        wake: wake::Waker<'d>,
    ) -> Self
    where
        S: Source<'o, 'r, 'c, 'b, 'd, ID>,
        'c: 'r,
    {
        match admit {
            core::Admit::Item(value) => Self::ready(source, value, credit, wake),
            core::Admit::Full => Self::full(source, cx, credit, wake),
            core::Admit::Error(error) => Self::Error(error),
        }
    }

    pub(in crate::adapter) fn channel<'o, 'r: 'o, K>(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, K>,
        cx: local::Access<'_, region::Identity<'d>>,
        index: fifo::Index<'c>,
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
        wake: wake::Waker<'d>,
    ) -> Self
    where
        K: core::Capability
            + core::Allows<core::State>
            + core::Allows<core::Egress>
            + core::Allows<core::Registry>,
    {
        if let Err(error) = route.core().permits().wait_channel(index, credit, &wake) {
            return Self::Error(failure::Runtime::from(error));
        }
        Self::pending(route, cx, credit)
    }

    fn ready<'o, 'r: 'o, S>(
        source: S,
        value: T,
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
        wake: wake::Waker<'d>,
    ) -> Self
    where
        S: Source<'o, 'r, 'c, 'b, 'd, ID>,
        'c: 'r,
    {
        source.core().outbound().sync();
        credit.clear();
        Self::Ready(value, wake)
    }

    fn full<'o, 'r: 'o, S>(
        source: S,
        cx: local::Access<'_, region::Identity<'d>>,
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
        wake: wake::Waker<'d>,
    ) -> Self
    where
        S: Source<'o, 'r, 'c, 'b, 'd, ID>,
        'c: 'r,
    {
        if let Err(error) = source.core().permits().wait_credit(credit, &wake) {
            return Self::Error(failure::Runtime::from(error));
        }
        Self::pending(source, cx, credit)
    }

    fn pending<'o, 'r: 'o, S>(
        source: S,
        cx: local::Access<'_, region::Identity<'d>>,
        credit: &mut queue::Registration<'o, 'd, fifo::Scope<'c>>,
    ) -> Self
    where
        S: Source<'o, 'r, 'c, 'b, 'd, ID>,
        'c: 'r,
    {
        match source.live(cx) {
            Ok(()) => Self::Pending,
            Err(error) => {
                credit.clear();
                Self::Error(error)
            }
        }
    }
}
