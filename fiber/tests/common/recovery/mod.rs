//! Deterministic multi-generation recovery contracts.

use std::convert::Infallible;

use dope::core::driver::life::recovery;

use crate::common::amqp::Tune;
pub const TUNE: Tune = Tune::new(16, 131_072, 0);

pub mod broker;
pub mod client;

mod profile;
pub use profile::{Profile, Startup};

pub mod fault;

pub fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
