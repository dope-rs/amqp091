use std::time::{Duration, Instant};

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope_fiber::abi::Fiber;
use dope_fiber::sleep::TimerExt as _;
use dope_net::Transport;

use crate::error::Error;
use crate::runtime::connection::handshake::Handshake;
use crate::runtime::handles::AmqpHandle;
use crate::wire::frame::FrameEncoder;

pub struct Heartbeat<'a, 'd, S, E>
where
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    handle: AmqpHandle<'a, 'd, S, E>,
}

impl<'a, 'd, S, E> Heartbeat<'a, 'd, S, E>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone> + 'd,
{
    pub fn new(handle: AmqpHandle<'a, 'd, S, E>) -> Self {
        Self { handle }
    }

    pub fn run(self) -> impl Fiber<'d, Output = Result<(), Error>> {
        let handle = self.handle;
        dope_fiber::fiber!('d => async move {
            Handshake::new(handle).await?;

            let interval_secs = {
                let conn = handle.connector();
                let Some(target) = conn.session().with_shared(|shared| shared.ready_conn) else {
                    return Err(Error::Closed);
                };
                conn.with_state(target, |state| state.negotiated().heartbeat)
                    .ok_or(Error::Closed)?
            };
            if interval_secs == 0 {
                return Ok(());
            }
            let interval = Duration::from_secs(interval_secs as u64);
            let dead_threshold = interval * 2;

            loop {
                handle.timer().sleep(interval).await;

                let conn = handle.connector();
                let state = conn.session().with_shared(|shared| {
                    if let Some(error) = shared.fatal.as_ref() {
                        return Err(error.clone());
                    }
                    shared.ready_conn.ok_or(Error::Closed)
                });
                let target = match state {
                    Ok(target) => target,
                    Err(error) => return Err(error),
                };

                let now = Instant::now();
                let timed_out = conn.session().with_shared(|shared| {
                    shared
                        .last_recv_at
                        .is_some_and(|last| now.duration_since(last) > dead_threshold)
                });
                if timed_out {
                    conn.session()
                        .with_shared(|shared| shared.record_recoverable_disconnect());
                    return Err(Error::Other("heartbeat timeout — peer silent".into()));
                }

                let frame = handle.encode(|out| FrameEncoder::new(out).write_heartbeat())?;
                conn.with_state(target, |state| state.try_enqueue(frame))
                    .ok_or(Error::Closed)?
                    .map_err(|_| Error::Other("egress queue full".into()))?;
            }
        })
    }
}
