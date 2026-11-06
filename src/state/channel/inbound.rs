use std::mem;

use o3::buffer::resident;

use crate::failure::{self, wire};
use crate::message::field::view;
use crate::message::property;
use crate::service::content::wait::wake::Wake;
use crate::service::content::{payload, pending};
use crate::state::channel::Phase;

#[derive(Default)]
#[repr(transparent)]
pub(crate) struct Inbound<'r, 'c, 'b, 'd>(Option<pending::Pending<'r, 'c, 'b, 'd>>);

impl<'r, 'c, 'b, 'd> Inbound<'r, 'c, 'b, 'd> {
    pub(crate) const fn is_idle(&self) -> bool {
        self.0.is_none()
    }

    pub(crate) fn clear(&mut self) {
        self.0 = None;
    }

    pub(crate) fn begin(
        &mut self,
        phase: Phase,
        entry: pending::Pending<'r, 'c, 'b, 'd>,
    ) -> Result<(), failure::Error> {
        match phase {
            Phase::Closed => {
                return Err(failure::Error::Protocol(wire::Fault::ContentMethodClosed));
            }
            Phase::Opening => {
                return Err(failure::Error::Protocol(wire::Fault::ContentMethodOpening));
            }
            Phase::Open => {}
            Phase::Closing => {
                return Err(failure::Error::Protocol(wire::Fault::ContentMethodClosing));
            }
        }
        if let Some(current) = self.0.as_ref() {
            let bound = current.is_bound();
            return Err(failure::Error::Protocol(
                wire::Fault::ContentMethodPending {
                    properties: bound,
                    body: bound,
                },
            ));
        }
        self.0 = Some(entry);
        Ok(())
    }

    pub(crate) fn header<E: From<failure::Error>>(
        &mut self,
        size: u64,
        maximum: usize,
        wake: Wake<'b, 'd>,
        bind: impl FnOnce(
            usize,
        ) -> Result<
            (
                property::Properties<resident::Shared<'d>, view::TableSpan>,
                bool,
                payload::Payload<'b, 'd>,
            ),
            E,
        >,
    ) -> Result<Option<pending::Complete<'r, 'c, 'b, 'd>>, E> {
        let Some(entry) = self.0.as_mut() else {
            return Err(E::from(failure::Error::Protocol(
                wire::Fault::ContentHeaderIdle,
            )));
        };
        if size > maximum as u64 {
            return Err(E::from(failure::Error::MessageTooLarge {
                size,
                max: maximum,
            }));
        }
        let size = size as usize;
        entry.bind(|| bind(size))?;
        Ok(pending::Pending::advance(&mut self.0, wake))
    }

    pub(crate) fn body<E: From<failure::Error>>(
        &mut self,
        length: usize,
        wake: Wake<'b, 'd>,
        store: impl FnOnce(&mut payload::Payload<'b, 'd>) -> Result<(), E>,
    ) -> Result<Option<pending::Complete<'r, 'c, 'b, 'd>>, E> {
        let Some(entry) = self.0.as_mut() else {
            return Err(E::from(failure::Error::Protocol(
                wire::Fault::ContentBodyIdle,
            )));
        };
        entry.body(length, store)?;
        Ok(pending::Pending::advance(&mut self.0, wake))
    }
}

const _: () = {
    assert!(mem::size_of::<Inbound<'_, '_, '_, '_>>() == 4 * mem::size_of::<usize>());
};
