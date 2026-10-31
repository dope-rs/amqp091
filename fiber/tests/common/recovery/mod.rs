//! Deterministic multi-generation recovery contracts.

use crate::common::amqp::Tune;
pub const TUNE: Tune = Tune::new(16, 131_072, 0);

pub mod broker;
pub mod client;

mod profile;
pub use profile::{Profile, Startup};

pub mod fault;
