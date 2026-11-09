use dope_test::harness::peer;
use std::time::Duration;

use dope_test::harness::peer::{AcceptScript, Spawn};

use crate::common::amqp::wire::{Frame, Method};
use crate::common::amqp::{Error, Peer, Tune};
use crate::common::client::Redial;

#[derive(Clone, Copy)]
pub enum Violation {
    InvalidEnd,
    UndefinedType(u8),
}

pub fn raw_peer<R>(
    fault: Violation,
    run: impl for<'scope> FnOnce(AcceptScript<'scope, Result<(), Error>>) -> R,
) -> Result<R, Spawn> {
    AcceptScript::with(
        move |acceptor| {
            let mut stream = acceptor.accept()?;
            stream.read_wait(Some(Duration::from_secs(5)))?;
            stream.write_wait(Some(Duration::from_secs(5)))?;
            Peer::new(&mut stream).open(Tune::new(16, 131_072, 0))?;
            Method::read(&mut stream)?.check(1, 20, 10)?;
            stream.write_all(&match fault {
                Violation::InvalidEnd => {
                    let mut wire = Frame::try_encode(8, 0, &[])?;
                    let length = wire.len();
                    *wire
                        .last_mut()
                        .ok_or(Error::Truncated { offset: 1, length })? = 0;
                    wire
                }
                Violation::UndefinedType(kind) => Frame::try_encode(kind, 0, &[])?,
            })?;
            let mut byte = [0_u8; 1];
            match stream.read(&mut byte) {
                Ok(0) => {}
                Err(peer::Error::Syscall(error)) if error.is_reset() => {}
                Err(error) => return Err(Error::Io(error)),
                Ok(bytes) => {
                    return Err(Error::UnexpectedRead {
                        context: "raw frame violation".to_owned(),
                        bytes,
                    });
                }
            }
            drop(stream);
            match Redial::observe(acceptor)? {
                Redial::Accepted => Err(Error::Redial),
                Redial::Elapsed => Ok(()),
            }
        },
        run,
    )
}
