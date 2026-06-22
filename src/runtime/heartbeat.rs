use std::time::{Duration, Instant};

use dope::fiber::Holding;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::manifold::timer::Timer;
use dope::transport::Transport;
use o3::buffer::Owned;

use crate::Error;
use crate::protocol::Session;
use crate::runtime::handshake::Handshake;
use crate::wire::FrameEncoder;

pub async fn heartbeat_loop<'d, S, E>(
    holding: Holding<'d, Connector<0, Session, S, E>>,
    timer: Holding<'d, Timer>,
) -> Result<(), Error>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone> + 'd,
{
    Handshake::new(holding).await?;

    let interval_secs = {
        let mut h = holding.hold();
        let Some(target) = h.session().shared.ready_conn else {
            return Err(Error::Closed);
        };
        let Some(slot) = h.as_mut().state_for(target) else {
            return Err(Error::Closed);
        };
        slot.conn_state().negotiated.heartbeat
    };
    if interval_secs == 0 {
        return Ok(());
    }
    let interval = Duration::from_secs(interval_secs as u64);
    let dead_threshold = interval * 2;

    loop {
        timer.sleep(interval).await;

        let mut h = holding.hold();
        if let Some(err) = h.session().shared.fatal.as_ref() {
            return Err(err.clone());
        }
        let Some(target) = h.session().shared.ready_conn else {
            return Err(Error::Closed);
        };

        let now = Instant::now();
        if let Some(last) = h.session().shared.last_recv_at
            && now.duration_since(last) > dead_threshold
        {
            let err = Error::Other("heartbeat timeout — peer silent".into());
            h.as_mut().session_mut().shared.record_failure(err.clone());
            return Err(err);
        }

        let Some(slot) = h.as_mut().state_for(target) else {
            return Err(Error::Closed);
        };
        let mut buf = Owned::with_capacity(8);
        FrameEncoder::write_heartbeat(&mut buf);
        let _ = slot.enqueue(buf.freeze());
        h.as_mut().request_flush(target);
    }
}
