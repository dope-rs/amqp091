use o3::buffer::resident;

use crate::failure;
use crate::message::field;
use crate::protocol::topology::{self, need};
use crate::protocol::wire::schema::{self, exchange, resource};
use crate::protocol::wire::text::short;
use crate::protocol::wire::{self, Put as _, outputs};
use crate::protocol::{class, method};
use crate::state::work::{self, requests};

pub(super) const DECLARE: method::Id = method::Id::new(class::EXCHANGE, 10);
pub(super) const DECLARE_OK: method::Id = method::Id::new(class::EXCHANGE, 11);
pub(super) const DELETE: method::Id = method::Id::new(class::EXCHANGE, 20);
pub(super) const DELETE_OK: method::Id = method::Id::new(class::EXCHANGE, 21);
pub(super) const BIND: method::Id = method::Id::new(class::EXCHANGE, 30);
pub(super) const BIND_OK: method::Id = method::Id::new(class::EXCHANGE, 31);
pub(super) const UNBIND: method::Id = method::Id::new(class::EXCHANGE, 40);
pub(super) const UNBIND_OK: method::Id = method::Id::new(class::EXCHANGE, 51);

pub struct Declare<'a> {
    pub exchange: short::Name<'a>,
    pub kind: short::Short<'a>,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: &'a field::Table,
}

pub struct Inspect<'a> {
    pub exchange: short::Name<'a>,
    pub kind: short::Short<'a>,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: &'a field::Table,
}

impl method::Spec for Declare<'_> {
    const ID: method::Id = DECLARE;
}

impl method::Args for Declare<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        write_declare(
            out,
            self.exchange,
            self.kind,
            [false, self.durable, self.auto_delete, self.internal],
            self.arguments,
        );
    }
}

impl method::Spec for Inspect<'_> {
    const ID: method::Id = DECLARE;
}

impl method::Args for Inspect<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        write_declare(
            out,
            self.exchange,
            self.kind,
            [true, self.durable, self.auto_delete, self.internal],
            self.arguments,
        );
    }
}

fn write_declare<S>(
    out: &mut S,
    name: short::Name<'_>,
    kind: short::Short<'_>,
    flags: [bool; 4],
    args: &field::Table,
) where
    S: wire::Put,
{
    out.put_u16(0);
    out.put_name(name);
    out.put_short(kind);
    let [probe, disk, auto, local] = flags;
    out.put_bits([probe, disk, auto, local, false]);
    args.encode(out);
}

impl method::Record<exchange::Declare> for Declare<'_> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, exchange::Declare>,
    ) -> outputs::Fields<'a, exchange::Declare, [schema::Offset; 2]> {
        out.put_u16(0);
        let mut out = out
            .put_field(exchange::Declare::NAME, self.exchange.into())
            .put_field(exchange::Declare::KIND, self.kind);
        out.put_bits([false, self.durable, self.auto_delete, self.internal, false]);
        out.put_table(self.arguments);
        out
    }
}

impl method::Request for Declare<'_> {
    type Response = DeclareOk;
}

impl<'d> method::Op<'d> for Declare<'_> {
    type Output<'b, Consumer> = ();
}

impl method::Request for Inspect<'_> {
    type Response = DeclareOk;
}

impl<'d> method::Op<'d> for Inspect<'_> {
    type Output<'b, Consumer> = ();
}

pub struct DeclareOk;

impl method::Spec for DeclareOk {
    const ID: method::Id = DECLARE_OK;
}

impl method::Unit for DeclareOk {
    const KIND: method::Kind = method::Kind::ExchangeDeclare;
}

pub struct Delete<'a> {
    pub exchange: short::Name<'a>,
    pub if_unused: bool,
}

impl method::Spec for Delete<'_> {
    const ID: method::Id = DELETE;
}

impl method::Args for Delete<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        out.put_u16(0);
        out.put_name(self.exchange);
        out.put_bits([self.if_unused, false]);
    }
}

impl method::Record<resource::Layout> for Delete<'_> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, resource::Layout>,
    ) -> outputs::Fields<'a, resource::Layout, [schema::Offset; 1]> {
        out.put_u16(0);
        let mut out = out.put_field(resource::Layout::NAME, self.exchange.into());
        out.put_bits([self.if_unused, false]);
        out
    }
}

impl method::Request for Delete<'_> {
    type Response = DeleteOk;
}

impl<'d> method::Op<'d> for Delete<'_> {
    type Output<'b, Consumer> = ();
}

pub struct DeleteOk;

impl method::Spec for DeleteOk {
    const ID: method::Id = DELETE_OK;
}

impl method::Unit for DeleteOk {
    const KIND: method::Kind = method::Kind::ExchangeDelete;
}

pub struct Bind<'a> {
    pub destination: short::Name<'a>,
    pub source: short::Name<'a>,
    pub routing_key: short::Short<'a>,
    pub arguments: &'a field::Table,
}

impl method::Spec for Bind<'_> {
    const ID: method::Id = BIND;
}

impl method::Args for Bind<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        write_binding(
            out,
            self.destination,
            self.source,
            self.routing_key,
            self.arguments,
        );
    }
}

fn write_binding<S>(
    out: &mut S,
    dst: short::Name<'_>,
    source: short::Name<'_>,
    route: short::Short<'_>,
    args: &field::Table,
) where
    S: wire::Put,
{
    out.put_u16(0);
    out.put_name(dst);
    out.put_name(source);
    out.put_short(route);
    out.put_bits([false]);
    args.encode(out);
}

impl method::Record<exchange::Bind> for Bind<'_> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, exchange::Bind>,
    ) -> outputs::Fields<'a, exchange::Bind, schema::Identity<3>> {
        out.put_u16(0);
        let mut out = out
            .put_field(exchange::Bind::DESTINATION, self.destination.into())
            .put_field(exchange::Bind::SOURCE, self.source.into())
            .put_field(exchange::Bind::ROUTING_KEY, self.routing_key);
        out.put_bits([false]);
        out.put_identity(self.arguments)
    }
}

impl method::Request for Bind<'_> {
    type Response = BindOk;
}

impl<'d> method::Op<'d> for Bind<'_> {
    type Output<'b, Consumer> = ();
}

pub struct BindOk;

impl method::Spec for BindOk {
    const ID: method::Id = BIND_OK;
}

impl method::Unit for BindOk {
    const KIND: method::Kind = method::Kind::ExchangeBind;
}

pub struct Unbind<'a> {
    pub destination: short::Name<'a>,
    pub source: short::Name<'a>,
    pub routing_key: short::Short<'a>,
    pub arguments: &'a field::Table,
}

impl method::Spec for Unbind<'_> {
    const ID: method::Id = UNBIND;
}

impl method::Args for Unbind<'_> {
    fn write_args<S: wire::Put>(&self, out: &mut S) {
        write_binding(
            out,
            self.destination,
            self.source,
            self.routing_key,
            self.arguments,
        );
    }
}

impl method::Record<exchange::Bind> for Unbind<'_> {
    fn record<'a>(
        &self,
        mut out: outputs::Fields<'a, exchange::Bind>,
    ) -> outputs::Fields<'a, exchange::Bind, schema::Identity<3>> {
        out.put_u16(0);
        let mut out = out
            .put_field(exchange::Bind::DESTINATION, self.destination.into())
            .put_field(exchange::Bind::SOURCE, self.source.into())
            .put_field(exchange::Bind::ROUTING_KEY, self.routing_key);
        out.put_bits([false]);
        out.put_identity(self.arguments)
    }
}

impl method::Request for Unbind<'_> {
    type Response = UnbindOk;
}

impl<'d> method::Op<'d> for Unbind<'_> {
    type Output<'b, Consumer> = ();
}

pub struct UnbindOk;

impl method::Spec for UnbindOk {
    const ID: method::Id = UNBIND_OK;
}

impl method::Unit for UnbindOk {
    const KIND: method::Kind = method::Kind::ExchangeUnbind;
}

impl requests::Seal for Inspect<'_> {}

impl<'d> requests::Source<'d> for Inspect<'_> {
    type Policy<const ID: u8> = requests::Plain;
}

impl requests::Seal for Declare<'_> {}

impl<'d> requests::Source<'d> for Declare<'_> {
    type Policy<const ID: u8> = requests::Tracked;
}

impl<'d, const ID: u8> requests::Capture<'d, ID> for Declare<'_> {
    type Schema = exchange::Declare;

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &resident::Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let name = view.field(exchange::Declare::NAME);
        let needs = need::Need::exchange(name);
        let (id, plan) = topo.preflight(&needs)?;
        let (spec, choice) = topo.bind(plan, {
            use crate::protocol::topology::records::exchange;

            exchange::Declare::capture(budget, view)
        })?;
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::ExchangeDeclare(spec, choice),
        })
    }
}

impl requests::Seal for Delete<'_> {}

impl<'d> requests::Source<'d> for Delete<'_> {
    type Policy<const ID: u8> = requests::Tracked;
}

impl<'d, const ID: u8> requests::Capture<'d, ID> for Delete<'_> {
    type Schema = resource::Layout;

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &resident::Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let name = view.field(resource::Layout::NAME);
        let needs = need::Need::none();
        let id = topo.resolve(&needs).0;
        let spec = {
            use crate::protocol::topology::records::exchange;

            exchange::Delete::capture(budget, name)?
        };
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::ExchangeDelete(spec),
        })
    }
}

impl requests::Seal for Bind<'_> {}

impl<'d> requests::Source<'d> for Bind<'_> {
    type Policy<const ID: u8> = requests::Tracked;
}

impl<'d, const ID: u8> requests::Capture<'d, ID> for Bind<'_> {
    type Schema = exchange::Bind;

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &resident::Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let dst = view.field(exchange::Bind::DESTINATION);
        let source = view.field(exchange::Bind::SOURCE);
        let route = view.field(exchange::Bind::ROUTING_KEY);
        let args = view.identity();
        let needs = need::Need::route(dst, source, route, args);
        let (id, plan) = topo.preflight(&needs)?;
        let (spec, choice) = topo.bind(plan, {
            use crate::protocol::topology::records::exchange;

            exchange::Bind::capture(budget, view)
        })?;
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::ExchangeBind(spec, choice),
        })
    }
}

impl requests::Seal for Unbind<'_> {}

impl<'d> requests::Source<'d> for Unbind<'_> {
    type Policy<const ID: u8> = requests::Tracked;
}

impl<'d, const ID: u8> requests::Capture<'d, ID> for Unbind<'_> {
    type Schema = exchange::Bind;

    fn capture<'c, 'b>(
        view: wire::Encoded<'_, Self::Schema>,
        budget: &resident::Budget<'d>,
        topo: &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error> {
        let needs = need::Need::none();
        let id = topo.resolve(&needs).0;
        let spec = {
            use crate::protocol::topology::records::exchange;

            exchange::Bind::capture(budget, view)?
        };
        Ok(work::Admit::Topology {
            identity: id,
            work: topology::Work::ExchangeUnbind(spec),
        })
    }
}
