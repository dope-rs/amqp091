use std::env;

use crate::common::amqp::app::vars::Error;

#[derive(Debug)]
pub enum Key {
    Host,
    Port,
    User,
    Password,
    Vhost,
}

impl Key {
    pub fn resolve(self, value: Result<String, env::VarError>) -> Result<String, Error> {
        let default = match self {
            Self::Host => None,
            Self::Port => Some("5672"),
            Self::User | Self::Password => Some("guest"),
            Self::Vhost => Some("/"),
        };
        match (value, default) {
            (Ok(value), _) => Ok(value),
            (Err(env::VarError::NotPresent), Some(default)) => Ok(default.to_owned()),
            (Err(cause), _) => Err(Error::Variable { key: self, cause }),
        }
    }
}
