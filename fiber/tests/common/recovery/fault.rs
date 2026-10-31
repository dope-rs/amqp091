use std::io::{ErrorKind, Read as _, Write as _};
use std::time::Duration;

use dope::core::platform::wake;
use dope_test::harness::peer::{AcceptScript, Spawn};

use crate::common::amqp::wire::Frame;
use crate::common::amqp::{Error, Peer, Tune};

#[derive(Clone, Copy)]
pub enum Violation {
    InvalidEnd,
    UndefinedType(u8),
}

pub fn raw_peer<R>(
    fault: Violation,
    ready: wake::Wait,
    done: Option<wake::Wait>,
    run: impl for<'scope> FnOnce(AcceptScript<'scope, Result<(), Error>>, wake::Wait) -> R,
) -> Result<R, Spawn> {
    let (observed, closed) = wake::Ends::blocking().map_err(Spawn::Wake)?.split();
    AcceptScript::with(
        move |acceptor| {
            let mut stream = acceptor.accept().map_err(Error::Io)?;
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .map_err(Error::Io)?;
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .map_err(Error::Io)?;
            Peer::new(&mut stream).open(Tune::new(16, 131_072, 0))?;
            ready
                .wait_timeout(Duration::from_secs(5))
                .map_err(Error::Wait)?;
            stream
                .write_all(&match fault {
                    Violation::InvalidEnd => {
                        let mut wire = Frame::try_encode(8, 0, &[])?;
                        let length = wire.len();
                        *wire
                            .last_mut()
                            .ok_or(Error::Truncated { offset: 1, length })? = 0;
                        wire
                    }
                    Violation::UndefinedType(kind) => Frame::try_encode(kind, 0, &[])?,
                })
                .map_err(Error::Io)?;
            let mut byte = [0_u8; 1];
            match stream.read(&mut byte) {
                Ok(0) => {}
                Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
                Err(error) => return Err(Error::Io(error)),
                Ok(bytes) => {
                    return Err(Error::UnexpectedRead {
                        context: "raw frame violation".to_owned(),
                        bytes,
                    });
                }
            }
            if let Some(done) = done {
                done.wait_timeout(Duration::from_secs(5))
                    .map_err(Error::Wait)?;
                if acceptor.try_accept().map_err(Error::Io)?.is_some() {
                    return Err(Error::Redial);
                }
            }
            closed.notify().map_err(Error::Notify)?;
            Ok(())
        },
        |broker| run(broker, observed),
    )
}
