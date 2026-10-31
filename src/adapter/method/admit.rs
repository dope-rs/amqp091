use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::channel::route;
use crate::adapter::core;
use crate::failure;
use crate::protocol::method::queue;
use crate::protocol::topology::need;
use crate::protocol::wire::schema::queues;
use crate::protocol::{method, topology, wire};
use crate::service::cancel;
use crate::state::channel::cancel::lanes;
use crate::state::work::requests::Defer;
use crate::state::work::{self, requests};

pub trait Admit<'d, M, const ID: u8>
where
    M: requests::Source<'d>,
{
    fn admit<'o, 'r: 'o, 'c: 'r, 'b: 'c>(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        access: local::Access<'_, region::Identity<'d>>,
        ticket: &cancel::Ticket<'o, 'd, lanes::Method, ID>,
        method: &M,
    ) -> Result<core::Admit<'c, 'b, 'd, fifo::Index<'c>, ID>, Defer>;
}

impl<'d, M, const ID: u8> Admit<'d, M, ID> for requests::Plain
where
    M: method::Op<'d> + requests::Source<'d, Policy<ID> = Self>,
{
    fn admit<'o, 'r: 'o, 'c: 'r, 'b: 'c>(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        access: local::Access<'_, region::Identity<'d>>,
        ticket: &cancel::Ticket<'o, 'd, lanes::Method, ID>,
        method: &M,
    ) -> Result<core::Admit<'c, 'b, 'd, fifo::Index<'c>, ID>, Defer> {
        let core = route.core();
        let id = route.lease().id();
        route.try_frame(
            access,
            |output| method::Encode::write(method, output, id),
            |route, _, ()| {
                commit(core, ticket, route, |topo| {
                    let needs = need::Need::direct(M::RETAIN);
                    let id = topo.resolve(&needs).0;
                    Ok(work::Admit::Direct(id))
                })
            },
        )
    }
}

impl<'d, M, const ID: u8> Admit<'d, M, ID> for requests::Keep
where
    M: method::Op<'d> + requests::Source<'d, Policy<ID> = Self> + requests::Prepare<'d, ID>,
{
    fn admit<'o, 'r: 'o, 'c: 'r, 'b: 'c>(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        access: local::Access<'_, region::Identity<'d>>,
        ticket: &cancel::Ticket<'o, 'd, lanes::Method, ID>,
        method: &M,
    ) -> Result<core::Admit<'c, 'b, 'd, fifo::Index<'c>, ID>, Defer> {
        let core = route.core();
        let id = route.lease().id();
        route.try_frame(
            access,
            |output| method::Encode::write(method, output, id),
            |route, _, ()| {
                commit(core, ticket, route, |topo| {
                    requests::Prepare::prepare(method, topo)
                })
            },
        )
    }
}

impl<'d, M, const ID: u8> Admit<'d, M, ID> for requests::Tracked
where
    M: method::Op<'d>
        + requests::Source<'d, Policy<ID> = Self>
        + requests::Capture<'d, ID>
        + method::Record<<M as requests::Capture<'d, ID>>::Schema>,
{
    fn admit<'o, 'r: 'o, 'c: 'r, 'b: 'c>(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        access: local::Access<'_, region::Identity<'d>>,
        ticket: &cancel::Ticket<'o, 'd, lanes::Method, ID>,
        method: &M,
    ) -> Result<core::Admit<'c, 'b, 'd, fifo::Index<'c>, ID>, Defer> {
        let core = route.core();
        record(route, access, ticket, method, |view, topo| {
            <M as requests::Capture<'d, ID>>::capture(view, core.budget(), topo)
        })
    }
}

impl<'a, 'd, const ID: u8> Admit<'d, queue::Declare<'a>, ID> for requests::Declaration {
    fn admit<'o, 'r: 'o, 'c: 'r, 'b: 'c>(
        route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
        access: local::Access<'_, region::Identity<'d>>,
        ticket: &cancel::Ticket<'o, 'd, lanes::Method, ID>,
        method: &queue::Declare<'a>,
    ) -> Result<core::Admit<'c, 'b, 'd, fifo::Index<'c>, ID>, Defer> {
        let core = route.core();
        record(route, access, ticket, method, |view, topo| {
            let name = view.field(queues::Declaration::QUEUE);
            if name.is_empty() && !method.exclusive {
                return Err(failure::Error::ServerNamedQueueNotExclusive);
            }
            <queue::Declare<'a> as requests::Capture<'d, ID>>::capture(view, core.budget(), topo)
        })
    }
}

fn record<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, M, const ID: u8>(
    route: route::Route<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
    access: local::Access<'_, region::Identity<'d>>,
    ticket: &cancel::Ticket<'o, 'd, lanes::Method, ID>,
    method: &M,
    plan: impl for<'a, 's, 'v> FnOnce(
        wire::Encoded<'a, <M as requests::Capture<'d, ID>>::Schema>,
        &'s mut topology::State<'v, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error>,
) -> Result<core::Admit<'c, 'b, 'd, fifo::Index<'c>, ID>, Defer>
where
    M: method::Op<'d>
        + requests::Capture<'d, ID>
        + method::Record<<M as requests::Capture<'d, ID>>::Schema>,
{
    let core = route.core();
    let id = route.lease().id();
    route.try_frame(
        access,
        |output| {
            wire::Encoded::<<M as requests::Capture<'d, ID>>::Schema>::method_frame(
                output,
                id,
                <M as method::Spec>::ID.bytes(),
                |output| method::Record::record(method, output),
            )
        },
        |route, frame, proof| {
            commit(core, ticket, route, |topo| {
                let view = proof.attach(frame).map_err(failure::Error::Protocol)?;
                plan(view, topo)
            })
        },
    )
}

fn commit<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>(
    core: core::Core<'o, 'r, 'c, 'b, 'd, ID, core::Methods>,
    ticket: &cancel::Ticket<'o, 'd, lanes::Method, ID>,
    route: route::View<'_, '_, '_, '_, 'r, 'c, 'b, 'd, ID>,
    plan: impl FnOnce(
        &mut topology::State<'_, 'c, 'b, 'd, ID>,
    ) -> Result<work::Admit<'c, 'b, 'd, ID>, failure::Error>,
) -> Result<fifo::Index<'c>, core::Reject<'b, 'd, Defer>> {
    let permit = match route.channel.method_state().reserve(ticket.identity()) {
        Ok(Some(permit)) => permit,
        Ok(None) => return Err(core::Reject::Defer(Defer::Wait)),
        Err(error) => return Err(core::Reject::Error(error)),
    };
    let admit = plan(route.topology)?;
    permit.commit(route.topology, core.identities(), admit)?;
    Ok(route.index.lane())
}
