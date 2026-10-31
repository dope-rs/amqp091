use dope::core::driver::schedule::clock;

use crate::adapter::conn::Ready;
use crate::failure;
use crate::state::shared;

/// Ephemeral view whose higher-ranked state borrow cannot escape one read.
#[derive(Clone, Copy)]
pub struct View<'state, 'r, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8> {
    pub(in crate::adapter::conn) state: &'state shared::Session<'r, 'c, 'b, 'd, ID>,
}

impl<'r, 'c: 'r, 'b: 'c, 'state, 'd: 'b, const ID: u8> View<'state, 'r, 'c, 'b, 'd, ID> {
    pub fn last_exit(self) -> Option<&'state failure::Exit<'c, 'b, 'd, ID>> {
        let life = &self.state.connection.lifecycle;
        life.status().exit().or(life.previous.as_ref())
    }

    pub fn generation(self) -> u64 {
        self.state.connection.lifecycle.generation()
    }

    pub fn ready_cycle(self) -> Option<u64> {
        if self.state.connection.lifecycle.status().ready().is_some() {
            Some(self.state.connection.lifecycle.generation())
        } else {
            None
        }
    }

    pub fn channels(self) -> usize {
        self.state.routes.len()
    }

    pub fn deadline(self) -> Option<(u64, clock::Deadline<'d>)> {
        self.state
            .connection
            .lifecycle
            .deadline()
            .map(|until| (self.state.connection.lifecycle.generation(), until))
    }

    pub fn ready(self) -> Option<Ready<'d, ID>> {
        self.state
            .connection
            .lifecycle
            .status()
            .ready()
            .map(Ready::from_raw)
    }

    pub fn is_blocked(self) -> bool {
        self.state.connection.blocked().is_some()
    }

    pub fn is_ready(self) -> bool {
        self.ready().is_some()
    }

    pub fn is_failed(self) -> bool {
        self.state.connection.lifecycle.status().is_failed()
    }
}
