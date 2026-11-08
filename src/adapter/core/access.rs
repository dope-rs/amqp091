use std::marker::PhantomData;

use crate::state::channel::cancel::lanes;

pub trait Capability {}

pub(in crate::adapter) trait Allows<P>: Capability {}

pub(in crate::adapter) struct Root;
pub(in crate::adapter) struct Channels;
pub(in crate::adapter) struct Conn;
pub(in crate::adapter) struct Observe;
pub(in crate::adapter) struct Command;
pub(in crate::adapter) struct Consumers;
pub struct Methods;
pub(in crate::adapter) struct Publish;

pub(in crate::adapter) struct State;
pub(in crate::adapter) struct Egress;
pub(in crate::adapter) struct Settings;
pub(in crate::adapter) struct Control;
pub(in crate::adapter) struct WaiterAccess;
pub(in crate::adapter) struct Registry;
pub(in crate::adapter) struct Issue<L>(PhantomData<fn() -> L>);
pub(in crate::adapter) struct Reap<L>(PhantomData<fn() -> L>);
pub(in crate::adapter) struct Project<T: Capability>(PhantomData<fn() -> T>);

impl Capability for Root {}
impl Allows<Project<Channels>> for Root {}
impl Allows<Project<Conn>> for Root {}
impl Allows<Project<Consumers>> for Root {}
impl Allows<Project<Methods>> for Root {}
impl Allows<Project<Publish>> for Root {}

impl Capability for Channels {}
impl Allows<State> for Channels {}
impl Allows<Egress> for Channels {}
impl Allows<WaiterAccess> for Channels {}
impl Allows<Registry> for Channels {}
impl Allows<Issue<lanes::Reservation>> for Channels {}
impl Allows<Reap<lanes::Reservation>> for Channels {}

impl Capability for Conn {}
impl Allows<Project<Observe>> for Conn {}
impl Allows<Project<Command>> for Conn {}

impl Capability for Observe {}
impl Allows<State> for Observe {}
impl Allows<WaiterAccess> for Observe {}

impl Capability for Command {}
impl Allows<State> for Command {}
impl Allows<Egress> for Command {}
impl Allows<WaiterAccess> for Command {}
impl Allows<Registry> for Command {}

impl Capability for Consumers {}
impl Allows<State> for Consumers {}
impl Allows<Egress> for Consumers {}
impl Allows<Registry> for Consumers {}
impl Allows<Project<Observe>> for Consumers {}

impl Capability for Methods {}
impl Allows<State> for Methods {}
impl Allows<Egress> for Methods {}
impl Allows<Control> for Methods {}
impl Allows<WaiterAccess> for Methods {}
impl Allows<Registry> for Methods {}
impl Allows<Issue<lanes::Method>> for Methods {}
impl Allows<Issue<lanes::Consumer>> for Methods {}
impl Allows<Reap<lanes::Method>> for Methods {}

impl Capability for Publish {}
impl Allows<State> for Publish {}
impl Allows<Egress> for Publish {}
impl Allows<Settings> for Publish {}
impl Allows<WaiterAccess> for Publish {}
impl Allows<Registry> for Publish {}
impl Allows<Issue<lanes::Publish>> for Publish {}
impl Allows<Issue<lanes::Confirm>> for Publish {}
impl Allows<Reap<lanes::Confirm>> for Publish {}
