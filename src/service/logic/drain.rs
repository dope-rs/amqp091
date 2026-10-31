use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::app;
use dope::manifold::client::close;
use dope::manifold::service::bindings::endpoint::State;
use o3::base::cell::{local, region};
use o3::depot::arena::slab::key;
use o3::mem::quota;

use crate::adapter::channel;
use crate::service::logic::session::SessionOps as _;
use crate::state::channel::cancel::Outcome;
use crate::state::channel::{consumer, methods};
use crate::state::{channels, epoch};
use crate::{failure, service};

enum Action<'d, const ID: u8> {
    Collect(channels::Lease<'d, ID>),
    RetireConsumer(channels::Lease<'d, ID>, consumer::Key),
    CloseAbandonedOpen(channels::Lease<'d, ID>),
    CloseTransport(epoch::Id<'d, ID>),
    Fail(epoch::Id<'d, ID>, failure::Error),
}

pub(super) fn run<'turn, 'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>(
    endpoint: &mut State<
        'o,
        'd,
        service::Port<'r, 'c, 'b, 'd, ID>,
        service::Logic<'o, 'd, ID>,
        KeyTag<ID>,
        1,
    >,
    work: app::Work<'turn, 'd>,
    region: &mut region::Token<region::Identity<'d>>,
) {
    loop {
        let (signal, turn) = match endpoint.port().coordination().drain(work) {
            quota::Admission::Item(item) => item,
            quota::Admission::Empty | quota::Admission::Exhausted => return,
        };
        let lease = signal.lease();
        let (kind, id) = signal.local();
        let action = turn.run(|| {
            endpoint.with_shared(region, |shared| {
                let (index, out) = shared
                    .routes
                    .project(lease, |view| {
                        view.channel
                            .cancel()
                            .reap(kind, id)
                            .map(|out| (view.index.lane(), out))
                    })
                    .flatten()?;
                if matches!(&out, Outcome::Method(_)) {
                    endpoint.port().admission().wake_channel(index);
                }
                let ready = shared.connection.lifecycle.status().ready();
                let (action, ctrl) = match out {
                    Outcome::Method(Err(failure::Error::Method(
                        key::Error::Free { .. }
                        | key::Error::Generation { .. }
                        | key::Error::Retired { .. },
                    ))) => (Action::Collect(lease), None),
                    Outcome::Method(Err(error)) => {
                        let Some(target) = ready else {
                            return Some((Action::Collect(lease), None));
                        };
                        (Action::Fail(target, error), None)
                    }
                    Outcome::Method(Ok(result)) => {
                        let ctrl = result
                            .control
                            .zip(ready)
                            .map(|(method, target)| (target, lease, method));
                        let action = match result.action {
                            methods::DetachAction::RetireConsumer(key) => {
                                Action::RetireConsumer(lease, key)
                            }
                            methods::DetachAction::CloseAbandonedOpen => {
                                Action::CloseAbandonedOpen(lease)
                            }
                            methods::DetachAction::None => Action::Collect(lease),
                        };
                        (action, ctrl)
                    }
                    Outcome::Consumer(key) => (Action::RetireConsumer(lease, key), None),
                    Outcome::Publish => {
                        let Some(target) = ready else {
                            return Some((Action::Collect(lease), None));
                        };
                        shared.retire(
                            target,
                            failure::Reason::Local(close::LocalOrigin::OperationCancelled),
                        );
                        (Action::CloseTransport(target), None)
                    }
                    Outcome::Confirm | Outcome::Reservation => (Action::Collect(lease), None),
                };
                Some((action, ctrl))
            })
        });
        let Some((action, next)) = action else {
            continue;
        };
        if let Some((target, lease, next)) = next {
            let result = endpoint
                .port()
                .control()
                .write(target, lease)
                .send(local::Access::new(region), next);
            if let Err(cause) = result {
                endpoint
                    .port()
                    .fail(local::Access::new(region), target, cause);
                continue;
            }
        }
        match action {
            Action::Fail(target, error) => {
                endpoint
                    .port()
                    .fail(local::Access::new(region), target, error);
            }
            Action::Collect(lease) => {
                endpoint.with_shared(region, |shared| {
                    shared.routes.collect(lease);
                });
            }
            Action::RetireConsumer(lease, key) => {
                endpoint.port().control().retire(
                    local::Access::new(region),
                    channel::Lease::from_raw(lease),
                    key,
                );
            }
            Action::CloseAbandonedOpen(lease) => {
                endpoint
                    .port()
                    .control()
                    .close(local::Access::new(region), channel::Lease::from_raw(lease));
            }
            Action::CloseTransport(target) => {
                endpoint
                    .port()
                    .transport
                    .close(target.token(), close::Reason::Protocol);
                endpoint.port().coordination().waiters().wake_all();
            }
        }
    }
}
