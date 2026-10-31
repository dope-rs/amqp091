use std::{env, net, num};

use dope_amqp091::failure;

mod key;
pub use key::Key;

pub const NAMES: [&str; 5] = [
    "RABBITMQ_HOST",
    "RABBITMQ_PORT",
    "RABBITMQ_USER",
    "RABBITMQ_PASSWORD",
    "RABBITMQ_VHOST",
];

#[derive(Debug)]
pub enum Error {
    Variable {
        key: Key,
        cause: env::VarError,
    },
    Host {
        value: String,
        cause: net::AddrParseError,
    },
    Port {
        value: String,
        cause: num::ParseIntError,
    },
    Config(failure::Error),
}
