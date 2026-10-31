use dope::core::driver;
use o3::alloc;

use crate::protocol::topology;
use crate::state::channel::methods::turn;
use crate::state::{layout, limits};

pub(crate) mod cancel;
pub(crate) mod change;
pub(crate) mod consumer;
pub(crate) mod facets;
pub(crate) mod inbound;
pub(crate) mod methods;
pub(crate) mod publish;
pub(crate) mod returns;

use consumer::delivery;
use publish::{confirms, streams};

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    #[default]
    Closed,
    Opening,
    Open,
    Closing,
}

#[must_use = "a freed route must wake the blocked route owner"]
#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Freed {
    #[default]
    Available,
    Route,
}

impl Freed {
    pub(crate) fn popped<T>(full: bool, value: Option<T>) -> (Option<T>, Self) {
        let freed = match (full, value.is_some()) {
            (true, true) => Self::Route,
            (true, false) | (false, _) => Self::Available,
        };
        (value, freed)
    }

    pub(crate) const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Route, _) | (_, Self::Route) => Self::Route,
            (Self::Available, Self::Available) => Self::Available,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Stores<'r, 'c, 'b, 'd> {
    pub(super) deliveries: &'r delivery::Store<'c, 'b, 'd>,
    pub(super) returns: &'r returns::Store<'c, 'b, 'd>,
}

impl<'r, 'c, 'b, 'd> Stores<'r, 'c, 'b, 'd> {
    pub(crate) const fn new(
        recv: &'r delivery::Store<'c, 'b, 'd>,
        ret: &'r returns::Store<'c, 'b, 'd>,
    ) -> Self {
        Self {
            deliveries: recv,
            returns: ret,
        }
    }

    pub(crate) const fn deliveries(self) -> &'r delivery::Store<'c, 'b, 'd> {
        self.deliveries
    }
}

pub(crate) struct Control {
    pub phase: Phase,
    flow_active: bool,
    confirms_enabled: bool,
}

impl Control {
    fn new() -> Self {
        Self {
            phase: Phase::Closed,
            flow_active: true,
            confirms_enabled: false,
        }
    }

    pub(crate) fn is_flow(&self) -> bool {
        self.flow_active
    }

    pub(crate) fn set_flow(&mut self, active: bool) {
        self.flow_active = active;
    }

    pub(crate) fn has_confirms(&self) -> bool {
        self.confirms_enabled
    }

    pub(crate) fn enable(&mut self) {
        self.confirms_enabled = true;
    }

    fn disable(&mut self) {
        self.confirms_enabled = false;
    }
}

pub(crate) struct Session<'r, 'c, 'b, 'd, const ID: u8> {
    stable: Stable<'c, 'b, 'd, ID>,
    epoch: Epoch<'r, 'c, 'b, 'd>,
    max_body: usize,
}

struct Stable<'c, 'b, 'd, const ID: u8> {
    methods: methods::Methods<'c, 'b, 'd, ID>,
    consumers: consumer::Set<'b, 'd, ID>,
    confirm: confirms::Ledger<'c, 'b, 'd>,
    publishing: streams::Flight,
}

struct Epoch<'r, 'c, 'b, 'd> {
    control: Control,
    inbound: inbound::Inbound<'r, 'c, 'b, 'd>,
    returns: returns::Queue,
}

impl<'r, 'c, 'b, 'd, const ID: u8> Session<'r, 'c, 'b, 'd, ID> {
    fn clear(&mut self, stores: Stores<'r, 'c, 'b, 'd>) -> Freed {
        self.epoch.inbound.clear();
        self.stable
            .consumers
            .drain(stores.deliveries)
            .merge(stores.returns.drain(&mut self.epoch.returns))
    }

    pub(crate) fn try_new(
        position: std::num::NonZero<u16>,
        limits: limits::Limits,
        plan: consumer::Plan,
        driver: driver::Reference<'d>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            stable: Stable {
                methods: methods::Methods::try_new(
                    limits.methods.user,
                    limits.methods.storage,
                    driver,
                )?,
                consumers: consumer::Set::try_new(plan, driver)?,
                confirm: confirms::Ledger::try_new(limits.confirms)?,
                publishing: streams::Flight::new(),
            },
            epoch: Epoch {
                control: Control::new(),
                inbound: inbound::Inbound::default(),
                returns: returns::Queue::new(position),
            },
            max_body: limits.resources.body,
        })
    }
}

impl<'r, 'c, 'b, 'd, const ID: u8> Session<'r, 'c, 'b, 'd, ID> {
    pub(crate) fn method_state(&mut self) -> facets::Method<'_, 'c, 'b, 'd, ID> {
        facets::Method {
            phase: &mut self.epoch.control.phase,
            methods: &mut self.stable.methods,
            consumers: &mut self.stable.consumers,
            publishing: &mut self.stable.publishing,
        }
    }

    pub(crate) fn publish(&mut self) -> facets::Publish<'_, 'c, 'b, 'd, ID> {
        facets::Publish {
            control: &mut self.epoch.control,
            methods: &mut self.stable.methods,
            confirm: &mut self.stable.confirm,
            publishing: &mut self.stable.publishing,
        }
    }

    pub(crate) fn confirm(&mut self) -> facets::Confirm<'_, 'c, 'b, 'd> {
        facets::Confirm {
            control: &mut self.epoch.control,
            confirm: &mut self.stable.confirm,
        }
    }

    pub(crate) fn cancel(&mut self) -> cancel::Access<'_, 'c, 'b, 'd, ID> {
        cancel::Access {
            control: &mut self.epoch.control,
            methods: &mut self.stable.methods,
            consumers: &mut self.stable.consumers,
            confirm: &mut self.stable.confirm,
            publishing: &mut self.stable.publishing,
        }
    }

    pub(crate) fn consumers(&mut self) -> &mut consumer::Set<'b, 'd, ID> {
        &mut self.stable.consumers
    }

    pub(crate) fn content(
        &mut self,
        stores: Stores<'r, 'c, 'b, 'd>,
    ) -> facets::Content<'_, 'r, 'c, 'b, 'd, ID> {
        facets::Content {
            phase: self.epoch.control.phase,
            inbound: &mut self.epoch.inbound,
            returns: &mut self.epoch.returns,
            consumers: &mut self.stable.consumers,
            stores,
            maximum: self.max_body,
        }
    }

    pub(crate) fn method_turn(
        &mut self,
        stores: Stores<'r, 'c, 'b, 'd>,
    ) -> turn::Turn<'_, 'r, 'c, 'b, 'd, ID> {
        turn::Turn::new(
            &mut self.stable.methods,
            &mut self.stable.consumers,
            stores.deliveries,
            self.stable.publishing.is_active(),
        )
    }

    pub(crate) fn control(&mut self) -> &mut Control {
        &mut self.epoch.control
    }

    pub(crate) fn protocol(&self) -> facets::Protocol {
        let want = match self.stable.methods.head() {
            methods::Head::Sent(kind) => Some(kind),
            methods::Head::Empty | methods::Head::Waiting => None,
        };
        facets::Protocol {
            phase: self.epoch.control.phase,
            expected: want,
            inbound_idle: self.epoch.inbound.is_idle(),
        }
    }

    pub(crate) fn has_replay(&self, topo: &topology::State<'_, 'c, 'b, 'd, ID>) -> bool {
        topo.graph().has_replay() || self.stable.consumers.read().has_replay()
    }

    pub(crate) fn change(
        &mut self,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
        stores: Stores<'r, 'c, 'b, 'd>,
        event: change::Event<'_, 'c, 'b, 'd>,
    ) -> Freed {
        match event {
            change::Event::Reopened => {
                let freed = self.clear(stores);
                self.stable.consumers.reopen();
                self.stable.confirm.retire_epoch();
                self.stable.publishing.retire_epoch();
                self.epoch.control.phase = Phase::Open;
                self.epoch.control.set_flow(true);
                self.epoch.control.disable();
                freed
            }
            event @ (change::Event::TransportEnded { .. } | change::Event::ChannelClosed) => {
                self.epoch.control.phase = Phase::Closed;
                self.stable.publishing.retire_epoch();
                let mut freed = self.clear(stores);
                if let change::Event::TransportEnded { cause } = event {
                    freed = freed.merge(self.method_turn(stores).fail(topo, cause));
                    if self.epoch.control.has_confirms() {
                        self.stable.confirm.fail_all(cause);
                    }
                } else {
                    self.stable.confirm.close();
                    self.stable.consumers.close();
                }
                self.epoch.returns.wake(stores.returns);
                freed
            }
        }
    }

    pub(crate) fn is_retiring(&self) -> bool {
        self.epoch.control.phase != Phase::Open
    }

    pub(crate) fn can_release(&self) -> bool {
        self.epoch.control.phase == Phase::Closed
            && self.stable.methods.is_quiescent()
            && self.stable.consumers.read().is_empty()
            && self.stable.confirm.is_empty()
            && !self.stable.publishing.is_active()
    }

    pub(super) fn release(&mut self) {
        self.stable.confirm.retire_epoch();
        self.stable.publishing.retire_epoch();
        self.epoch.control = Control::new();
    }
}

const _: () = {
    assert!(std::mem::size_of::<Freed>() == 1);
    layout::Layout::u64_words(42).prove::<Stable<'_, '_, '_, 0>>();
    layout::Layout::u64_words(38).prove::<Epoch<'_, '_, '_, '_>>();
    layout::Layout::u64_words(81).prove::<Session<'_, '_, '_, '_, 0>>();
};
