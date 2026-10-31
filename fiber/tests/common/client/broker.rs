use std::io::{ErrorKind, Read as _, Write as _};
use std::time::Duration;

use dope::core::platform::wake;
use dope_test::harness::peer::{AcceptScript, Spawn, TcpScript};

use crate::common::amqp::wire::Method;
use crate::common::amqp::{Error, Greeting, Peer};
use crate::common::client::{Redial, TUNE, check_no_frame};

pub fn close<R>(
    ack: bool,
    run: impl for<'scope> FnOnce(AcceptScript<'scope, Result<Redial, Error>>) -> R,
) -> Result<R, Spawn> {
    AcceptScript::with(
        move |acceptor| {
            let mut stream = acceptor.accept().map_err(Error::Io)?;
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .map_err(Error::Io)?;
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .map_err(Error::Io)?;
            Peer::new(&mut stream).open(TUNE)?;
            Method::read(&mut stream)
                .map_err(Error::Io)?
                .check(0, 10, 50)?;
            if ack {
                stream
                    .write_all(&Method::try_encode(0, 10, 51, &[])?)
                    .map_err(Error::Io)?;
            }
            drop(stream);

            Redial::observe(acceptor).map_err(Error::Io)
        },
        run,
    )
}

pub fn handshake<R>(
    version: (u8, u8),
    locales: &[u8],
    run: impl for<'scope> FnOnce(AcceptScript<'scope, Result<Redial, Error>>) -> R,
) -> Result<R, Spawn> {
    AcceptScript::with(
        move |acceptor| {
            let mut stream = acceptor.accept().map_err(Error::Io)?;
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .map_err(Error::Io)?;
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .map_err(Error::Io)?;

            Peer::new(&mut stream)
                .preface()?
                .start(Greeting::new(version.0, version.1, b"PLAIN", locales))?;
            drop(stream);

            Redial::observe(acceptor).map_err(Error::Io)
        },
        run,
    )
}

pub fn illegal<R>(
    channel: u16,
    class: u16,
    method: u16,
    args: &[u8],
    done: wake::Notify,
    run: impl for<'scope> FnOnce(AcceptScript<'scope, Result<(bool, bool, Redial), Error>>) -> R,
) -> Result<R, Spawn> {
    AcceptScript::with(
        move |acceptor| {
            let mut stream = acceptor.accept().map_err(Error::Io)?;
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .map_err(Error::Io)?;
            stream
                .write_wait(Some(Duration::from_secs(5)))
                .map_err(Error::Io)?;

            Peer::new(&mut stream)
                .preface()?
                .method(channel, class, method, args)?;

            let mut response = [0u8; 64];
            let (sent, closed) = match stream.read(&mut response) {
                Ok(0) => (false, true),
                Ok(_) => (true, false),
                Err(error) => {
                    let kind = error.kind();
                    if !matches!(
                        kind,
                        ErrorKind::ConnectionReset
                            | ErrorKind::ConnectionAborted
                            | ErrorKind::WouldBlock
                            | ErrorKind::TimedOut
                    ) {
                        return Err(Error::Io(error));
                    }
                    (
                        false,
                        matches!(
                            kind,
                            ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
                        ),
                    )
                }
            };
            drop(stream);
            let redial = Redial::observe(acceptor).map_err(Error::Io)?;
            done.notify().map_err(Error::Notify)?;
            Ok((sent, closed, redial))
        },
        run,
    )
}

pub fn rejection<R>(
    run: impl for<'scope> FnOnce(TcpScript<'scope, Result<(), Error>>) -> R,
) -> Result<R, Spawn> {
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE)?;
            Method::read(stream).map_err(Error::Io)?.check(1, 20, 10)?;
            stream
                .write_all(&Method::try_encode(1, 20, 11, &[0, 0, 0, 0])?)
                .map_err(Error::Io)?;
            check_no_frame(stream, "exhausted topology retention")
        },
        run,
    )
}
