use std::io;
use std::time::Duration;

use dope_test::harness::peer::TcpAcceptor;

#[derive(Debug, PartialEq, Eq)]
pub enum Redial {
    Accepted,
    Elapsed,
}

impl Redial {
    pub(crate) fn observe(acceptor: &mut TcpAcceptor<'_>) -> io::Result<Self> {
        Ok(match acceptor.accept_for(Duration::from_millis(1_250))? {
            Some(_) => Self::Accepted,
            None => Self::Elapsed,
        })
    }
}
