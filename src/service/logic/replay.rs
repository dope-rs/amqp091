use dope::core::driver::route::KeyTag;
use dope::core::driver::schedule::app;
use dope::manifold::service::bindings::endpoint::State;
use o3::base::cell::{local, region};

use crate::service::logic::session::SessionOps as _;
use crate::service::{self, port};

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
        let state = endpoint.with_shared(region, |shared| {
            (shared.recovery.target(), shared.recovery.is_runnable())
        });
        let (Some(target), true) = state else {
            return;
        };
        let Some(turn) = work.permit() else {
            endpoint.port().coordination().waiters().wake_flow();
            return;
        };
        match turn.run(|| endpoint.port().replay(local::Access::new(region), target)) {
            service::Admit::Item(port::Replay::Frame) => {}
            service::Admit::Item(port::Replay::Ready) => {
                endpoint.port().coordination().waiters().wake_state();
                return;
            }
            service::Admit::Item(port::Replay::Idle) | service::Admit::Inactive => return,
            service::Admit::Unavailable => {
                endpoint.port().coordination().waiters().wake_flow();
                return;
            }
            service::Admit::Error(error) => {
                endpoint
                    .port()
                    .fail(local::Access::new(region), target, error);
                return;
            }
        }
    }
}
