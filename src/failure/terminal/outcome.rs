use std::mem;

use o3::mem::rc;

use crate::failure::{self, terminal};

pub(crate) enum Outcome<'c, 'b, 'd> {
    Retain(rc::Error),
    Terminal(terminal::Lease<'c, 'b, 'd>),
}

impl<'c, 'b, 'd> Outcome<'c, 'b, 'd> {
    pub(crate) fn retain(cause: &terminal::Lease<'c, 'b, 'd>) -> Self {
        match cause.try_clone() {
            Ok(cause) => Self::Terminal(cause),
            Err(error) => Self::Retain(error),
        }
    }

    pub(crate) fn branded<const ID: u8>(self) -> failure::Runtime<'c, 'b, 'd, ID> {
        match self {
            Self::Retain(error) => failure::Runtime::Local(failure::Error::Retain(error)),
            Self::Terminal(cause) => failure::Runtime::terminal(cause),
        }
    }

    pub(crate) fn replace(&mut self, cause: &terminal::Lease<'c, 'b, 'd>) {
        let Self::Terminal(current) = self else {
            return;
        };
        if current.generation() == cause.generation() && !current.is_same(cause) {
            *self = Self::retain(cause);
        }
    }
}

const _: () = assert!(
    mem::size_of::<Outcome<'static, 'static, 'static>>()
        == mem::size_of::<terminal::Lease<'static, 'static, 'static>>()
);
