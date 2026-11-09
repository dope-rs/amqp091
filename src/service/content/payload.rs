use std::mem::size_of;

use o3::base::num::bounded;
use o3::buffer::hold::scope;
use o3::buffer::write::consume;
use o3::buffer::{self, Residency as _, hold, pool};
use o3::depot;
use o3::depot::arena::linked::{Iter, slots};
use o3::depot::queue::pool::fifo::Fifo;
use o3::mem::{Lease, credit};

use crate::failure::{self, wire};
use crate::service::content::{self, pending};

pub(crate) enum Payload<'b, 'd> {
    Compact(
        pool::Limit<pool::Span<pool::owners::Plain, pool::Borrowed<'b>>, Lease<credit::Return<'b>>>,
    ),
    Segments {
        remaining: usize,
        parts: Parts<'b, 'd>,
        budget: Option<Lease<credit::Return<'b>>>,
    },
}

pub(crate) struct Parts<'b, 'd> {
    first: Option<hold::Retained<'d>>,
    rest: Fifo<'b, hold::Retained<'d>>,
    resident: bool,
    fragments: bounded::U32<0, { u32::MAX - 1 }>,
}

#[derive(Clone, Copy)]
pub(super) struct Release {
    pub(super) bytes: usize,
    pub(super) resident: bool,
    pub(super) complete: bool,
    pub(super) fragments: u32,
}

fn release(
    budget: Option<&Lease<credit::Return<'_>>>,
    resident: bool,
    complete: bool,
    count: u32,
) -> Release {
    Release {
        bytes: budget.map_or(0, |lease| lease.item().get()),
        resident,
        complete,
        fragments: count,
    }
}

impl<'b, 'd> Payload<'b, 'd> {
    pub(crate) fn new(
        compact: &'b pool::Pool,
        nodes: &'b depot::raw::Arena<hold::Retained<'d>>,
        budget: Option<Lease<credit::Return<'b>>>,
    ) -> Self {
        let (remain, budget) = match budget {
            Some(budget) => {
                let amount = budget.item().get();
                match pool::Limit::try_borrow(compact, amount, budget) {
                    Ok(cursor) => return Self::Compact(cursor),
                    Err((budget, pool::Error::Capacity(_) | pool::Error::Empty)) => {
                        (amount, Some(budget))
                    }
                }
            }
            None => (0, None),
        };
        Self::Segments {
            remaining: remain,
            parts: Parts {
                first: None,
                rest: nodes.fifo(),
                resident: false,
                fragments: bounded::U32::exact::<0>(),
            },
            budget,
        }
    }

    /// Retention preserves the selected length and ownership.
    pub(crate) fn store<'input, R, const ID: u8>(
        &mut self,
        source: scope::Deferred<'input, 'd, R>,
        policy: hold::Policy,
        store: &content::Store<'_, '_, '_, 'd, ID>,
    ) -> Result<(), pending::Error>
    where
        R: buffer::Cursor + hold::raw::Retain<'d> + 'input,
    {
        match self {
            Self::Compact(cursor) => {
                cursor.try_extend(source.as_slice()).map_err(|error| {
                    failure::Error::Protocol(wire::Fault::ContentBodyOverflow(error))
                })?;
            }
            Self::Segments {
                remaining: count,
                parts,
                ..
            } => {
                let source = source.bind(policy);
                let expected = source.len();
                let left = *count;
                consume::Error::check(expected, left).map_err(|error| {
                    failure::Error::Protocol(wire::Fault::ContentBodyOverflow(error))
                })?;
                if expected == 0 {
                    return Ok(());
                }
                let vacant = parts.vacant().ok_or_else(|| store.wait.fragment())?;
                let chunk = store.retain(source)?;
                vacant.insert(chunk);
                *count = left - expected;
            }
        }
        Ok(())
    }

    pub(super) fn remaining(&self) -> usize {
        match self {
            Self::Compact(cursor) => cursor.remaining(),
            Self::Segments {
                remaining: left, ..
            } => *left,
        }
    }

    pub(super) fn release(&self) -> Release {
        match self {
            Self::Compact(cursor) => {
                release(Some(cursor.brand()), false, cursor.remaining() == 0, 0)
            }
            Self::Segments {
                remaining,
                parts,
                budget,
            } => release(
                budget.as_ref(),
                parts.resident,
                *remaining == 0,
                parts.fragments.get(),
            ),
        }
    }
}

impl<'b, 'd> Parts<'b, 'd> {
    fn vacant(&mut self) -> Option<Vacant<'_, 'b, 'd>> {
        if self.first.is_none() {
            return Some(Vacant::First {
                first: &mut self.first,
                resident: &mut self.resident,
            });
        }
        Some(Vacant::Rest {
            entry: self.rest.vacant()?,
            resident: &mut self.resident,
            fragments: &mut self.fragments,
        })
    }
}

impl<'b, 'd> Payload<'b, 'd> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Compact(cursor) => cursor.brand().item().get(),
            Self::Segments { budget, .. } => budget.as_ref().map_or(0, |lease| lease.item().get()),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn as_slice(&self) -> Option<&[u8]> {
        match self {
            Self::Compact(cursor) => Some(cursor.as_ref()),
            Self::Segments { parts, .. } if parts.rest.is_empty() => {
                Some(parts.first.as_ref().map_or(&[], |owner| owner.as_slice()))
            }
            Self::Segments { .. } => None,
        }
    }

    pub(crate) fn chunks<'a>(&'a self) -> Chunks<'a, 'd> {
        match self {
            Self::Compact(cursor) => Chunks::Compact(Some(cursor.as_ref())),
            Self::Segments { parts, .. } => Chunks::Segments {
                first: parts.first.as_ref(),
                rest: parts.rest.iter(),
            },
        }
    }
}

pub(crate) enum Chunks<'a, 'd> {
    Compact(Option<&'a [u8]>),
    Segments {
        first: Option<&'a hold::Retained<'d>>,
        rest: Iter<'a, hold::Retained<'d>>,
    },
}

impl<'a> Iterator for Chunks<'a, '_> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Compact(first) => first.take(),
            Self::Segments { first, rest } => first
                .take()
                .or_else(|| rest.next())
                .map(|owner| owner.as_slice()),
        }
    }
}

enum Vacant<'a, 'b, 'd> {
    First {
        first: &'a mut Option<hold::Retained<'d>>,
        resident: &'a mut bool,
    },
    Rest {
        entry: slots::Vacant<hold::Retained<'d>, &'a mut Fifo<'b, hold::Retained<'d>>>,
        resident: &'a mut bool,
        fragments: &'a mut bounded::U32<0, { u32::MAX - 1 }>,
    },
}

impl<'d> Vacant<'_, '_, 'd> {
    fn insert(self, chunk: hold::Retained<'d>) {
        let retained = chunk.resident() != 0;
        match self {
            Self::First { first, resident } => {
                *first = Some(chunk);
                *resident |= retained;
            }
            Self::Rest {
                entry,
                resident,
                fragments: count,
            } => {
                entry.push_back(chunk);
                *resident |= retained;
                // SAFETY: this exact FIFO gained its exclusively reserved node;
                // its count remains within the arena's u32::MAX - 1 capacity.
                *count = unsafe { bounded::raw::u32(count.get() + 1) };
            }
        }
    }
}

const _: () = {
    assert!(size_of::<Release>() == 2 * size_of::<usize>());
    assert!(size_of::<Payload<'static, 'static>>() == 10 * size_of::<usize>());
    assert!(size_of::<Chunks<'static, 'static>>() == 3 * size_of::<usize>());
};
