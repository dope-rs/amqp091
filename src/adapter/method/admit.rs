use o3::base::cell::{local, region};
use o3::depot::queue::resident::fifo;

use crate::adapter::channel::route;
use crate::adapter::core;
use crate::failure;
use crate::protocol::topology::need;
use crate::protocol::wire::text::short;
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
            |output, _| method::Encode::write(method, output, id),
            |route, _, ()| {
                commit::<M, ID>(core, ticket, route, |_| {
                    Ok(work::Admit::Direct(need::IdentityPlan::None))
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
            |output, _| method::Encode::write(method, output, id),
            |route, _, ()| {
                commit::<M, ID>(core, ticket, route, |topo| {
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
        let id = route.lease().id();
        route.try_frame(
            access,
            |output, access| {
                let mut encode = |queue: Option<short::Name<'_>>| {
                    wire::Encoded::<<M as requests::Capture<'d, ID>>::Schema>::method_frame(
                        output,
                        id,
                        <M as method::Spec>::ID.bytes(),
                        |output| match queue {
                            Some(queue) => {
                                <M as requests::Capture<'d, ID>>::record(method, output, queue)
                            }
                            None => method::Record::record(method, output),
                        },
                    )
                };
                if !<M as requests::Capture<'d, ID>>::reference(method)
                    .is_some_and(|name| name.is_empty())
                {
                    return encode(None);
                }
                core.shared().write(access, |state| {
                    if state.connection.lifecycle.status().ready() != Some(route.epoch().raw()) {
                        return encode(None);
                    }
                    state
                        .routes
                        .project(route.lease().raw(), |view| {
                            let graph = view.topology.graph();
                            let queue = match graph.current_state() {
                                topology::records::queue::Current::Named { name, live: false } => {
                                    Some(name.name())
                                }
                                _ => None,
                            };
                            encode(queue)
                        })
                        .unwrap_or_else(|| encode(None))
                })
            },
            |route, frame, proof| {
                commit::<M, ID>(core, ticket, route, |topo| {
                    let view = proof.attach(frame).map_err(failure::Error::Protocol)?;
                    <M as requests::Capture<'d, ID>>::capture(method, view, core.budget(), topo)
                })
            },
        )
    }
}

fn commit<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, M: method::Op<'d>, const ID: u8>(
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
    if matches!(<M::Response as method::Reply>::KIND, method::Kind::CloseOk) {
        route.channel.close_sent(core.stores());
    }
    Ok(route.index.lane())
}
