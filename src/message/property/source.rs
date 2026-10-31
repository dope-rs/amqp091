use std::num;

use hold::scope;
use o3::buffer;
use o3::buffer::{hold, resident};

use crate::message::field::view;
use crate::message::property::{Properties, Text};

pub(crate) struct Source<'input, 'd> {
    bytes: &'input [u8],
    budget: &'input resident::Budget<'d>,
}

pub(crate) enum State<'input, 'd> {
    Empty(Properties<resident::Shared<'d>, view::TableSpan>),
    Resident(Admission<'input, 'd>),
}

pub(crate) struct Admission<'input, 'd> {
    properties: Properties<Source<'input, 'd>, view::TableSpan>,
    demand: num::NonZero<usize>,
}

impl<'input, 'd> Source<'input, 'd> {
    pub(crate) fn new<R: buffer::Cursor + 'input>(source: scope::Deferred<'input, 'd, R>) -> Self {
        Self {
            bytes: source.as_slice(),
            budget: source.budget(),
        }
    }

    fn try_compact(self) -> Result<resident::Shared<'d>, hold::Error> {
        resident::Exact::try_copy(self.budget, self.bytes)
            .map(resident::Exact::into_shared)
            .map_err(hold::Error::Resident)
    }
}

impl AsRef<[u8]> for Source<'_, '_> {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

impl<'input, 'd> Properties<Source<'input, 'd>, view::TableSpan> {
    pub(crate) fn into_state(self) -> State<'input, 'd> {
        let Self {
            text,
            headers,
            delivery_mode: mode,
            priority,
            timestamp: time,
        } = self;
        let (text, keys) = text.into_parts();
        match num::NonZero::<usize>::new(text.bytes.len()) {
            Some(demand) if headers.is_some() || keys.iter().any(|key| key.is_some()) => {
                State::Resident(Admission {
                    properties: Properties {
                        text: Text::new(text, keys),
                        headers,
                        delivery_mode: mode,
                        priority,
                        timestamp: time,
                    },
                    demand,
                })
            }
            _ => State::Empty(Properties {
                text: Text::new(resident::Shared::new(), keys),
                headers,
                delivery_mode: mode,
                priority,
                timestamp: time,
            }),
        }
    }
}

impl<'input, 'd> Admission<'input, 'd> {
    pub(crate) const fn demand(&self) -> num::NonZero<usize> {
        self.demand
    }

    pub(crate) fn try_compact(
        self,
    ) -> Result<Properties<resident::Shared<'d>, view::TableSpan>, hold::Error> {
        let Properties {
            text,
            headers,
            delivery_mode: mode,
            priority,
            timestamp: time,
        } = self.properties;
        let (text, keys) = text.into_parts();
        Ok(Properties {
            text: Text::new(text.try_compact()?, keys),
            headers,
            delivery_mode: mode,
            priority,
            timestamp: time,
        })
    }
}
