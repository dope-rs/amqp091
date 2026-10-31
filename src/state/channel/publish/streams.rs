use std::num;

use crate::failure::{self, wire};

pub(crate) struct Flight {
    last: Option<Key>,
    active: Option<Key>,
}

pub(crate) struct Active<'a> {
    active: &'a mut Option<Key>,
}

pub(crate) struct Vacant<'a> {
    flight: &'a mut Flight,
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Key(num::NonZero<u64>);

impl Key {
    const FIRST: Self = Self(num::NonZero::<u64>::MIN);

    pub(crate) const fn new(raw: num::NonZero<u64>) -> Self {
        Self(raw)
    }

    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl Active<'_> {
    pub(crate) fn finish(self) {
        *self.active = None;
    }
}

impl Vacant<'_> {
    pub(crate) fn begin<R>(
        self,
        admit: impl FnOnce(Key) -> Result<R, failure::Error>,
    ) -> Result<R, failure::Error> {
        let key = match self.flight.last {
            Some(last) => last
                .next()
                .ok_or(failure::Error::Capacity(wire::Capacity::PublishGeneration))?,
            None => Key::FIRST,
        };
        let admitted = admit(key)?;
        self.flight.last = Some(key);
        self.flight.active = Some(key);
        Ok(admitted)
    }
}

impl Flight {
    pub(crate) const fn new() -> Self {
        Self {
            last: None,
            active: None,
        }
    }

    pub(crate) fn vacant(&mut self) -> Option<Vacant<'_>> {
        if self.active.is_some() {
            return None;
        }
        Some(Vacant { flight: self })
    }

    pub(crate) fn get(&mut self, key: Key) -> Option<Active<'_>> {
        if self.active != Some(key) {
            return None;
        }
        Some(Active {
            active: &mut self.active,
        })
    }

    pub(crate) fn is_owner(&self, key: Key) -> bool {
        self.active == Some(key)
    }

    pub(crate) const fn is_active(&self) -> bool {
        self.active.is_some()
    }

    pub(crate) fn retire_epoch(&mut self) {
        self.active = None;
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Key>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Key>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Flight>() == 2 * mem::size_of::<u64>());
    assert!(mem::size_of::<Active<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Vacant<'static>>() == mem::size_of::<usize>());
};

impl From<Key> for num::NonZero<u64> {
    fn from(value: Key) -> Self {
        value.0
    }
}
