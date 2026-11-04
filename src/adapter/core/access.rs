use std::marker::PhantomData;

use crate::state::channel::cancel::lanes;

pub trait Seal {}
pub(in crate::adapter) trait Permit<P> {}

pub trait Capability: Seal {}

impl<K: Seal> Capability for K {}

pub(in crate::adapter) trait Allows<P>: Capability + Permit<P> {}

impl<K, P> Allows<P> for K where K: Capability + Permit<P> {}

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

impl Seal for Root {}
impl Permit<Project<Channels>> for Root {}
impl Permit<Project<Conn>> for Root {}
impl Permit<Project<Consumers>> for Root {}
impl Permit<Project<Methods>> for Root {}
impl Permit<Project<Publish>> for Root {}

impl Seal for Channels {}
impl Permit<State> for Channels {}
impl Permit<Egress> for Channels {}
impl Permit<WaiterAccess> for Channels {}
impl Permit<Registry> for Channels {}
impl Permit<Issue<lanes::Reservation>> for Channels {}
impl Permit<Reap<lanes::Reservation>> for Channels {}

impl Seal for Conn {}
impl Permit<Project<Observe>> for Conn {}
impl Permit<Project<Command>> for Conn {}

impl Seal for Observe {}
impl Permit<State> for Observe {}
impl Permit<WaiterAccess> for Observe {}

impl Seal for Command {}
impl Permit<State> for Command {}
impl Permit<Egress> for Command {}
impl Permit<WaiterAccess> for Command {}
impl Permit<Registry> for Command {}

impl Seal for Consumers {}
impl Permit<State> for Consumers {}
impl Permit<Egress> for Consumers {}
impl Permit<Control> for Consumers {}
impl Permit<WaiterAccess> for Consumers {}
impl Permit<Registry> for Consumers {}
impl Permit<Project<Observe>> for Consumers {}

impl Seal for Methods {}
impl Permit<State> for Methods {}
impl Permit<Egress> for Methods {}
impl Permit<Control> for Methods {}
impl Permit<WaiterAccess> for Methods {}
impl Permit<Registry> for Methods {}
impl Permit<Issue<lanes::Method>> for Methods {}
impl Permit<Issue<lanes::Consumer>> for Methods {}
impl Permit<Reap<lanes::Method>> for Methods {}

impl Seal for Publish {}
impl Permit<State> for Publish {}
impl Permit<Egress> for Publish {}
impl Permit<Settings> for Publish {}
impl Permit<Control> for Publish {}
impl Permit<WaiterAccess> for Publish {}
impl Permit<Registry> for Publish {}
impl Permit<Issue<lanes::Publish>> for Publish {}
impl Permit<Issue<lanes::Confirm>> for Publish {}
impl Permit<Reap<lanes::Confirm>> for Publish {}
