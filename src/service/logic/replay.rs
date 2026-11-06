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
    let Some(target) = endpoint.with_shared(region, |shared| shared.recovery.runnable()) else {
        return;
    };
    let Some(turn) = work.permit() else {
        // A deferred plan can finish without output, even with held input or
        // no buffer. Resource pressure instead waits for an existing I/O turn.
        work.defer();
        return;
    };
    match turn.run(|| endpoint.port().replay(local::Access::new(region), target)) {
        service::Admit::Item(port::Replay::Frame | port::Replay::Idle)
        | service::Admit::Inactive
        | service::Admit::Unavailable => {}
        service::Admit::Item(port::Replay::Ready) => {
            endpoint.port().coordination().waiters().wake_state();
        }
        service::Admit::Error(error) => {
            endpoint
                .port()
                .fail(local::Access::new(region), target, error);
        }
    }
}
