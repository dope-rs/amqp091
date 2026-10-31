pub(crate) use field::Field;

use crate::failure::wire::Fault;
use crate::protocol::wire::Fingerprint;

mod field;

pub(crate) mod consume;
pub(crate) mod exchange;
pub(crate) mod queues;
pub(crate) mod resource;

pub(crate) const MAX_FIELDS: usize = 3;

#[derive(Clone, Copy, Default)]
pub(crate) struct Offset(u32);

impl Offset {
    pub(crate) const fn new(value: u32) -> Self {
        Self(value)
    }

    pub(crate) const fn get(self) -> u32 {
        self.0
    }
}

pub(crate) trait Proof: Copy {
    type Retained: Stored;

    const FIELDS: usize;

    fn offsets(&self) -> &[Offset];

    fn validate(&self, cap: usize) -> Result<(), Fault>;

    fn retained(self) -> Self::Retained;
}

pub(crate) trait Identified: Proof {
    fn start(&self) -> Offset;

    fn fingerprint(&self) -> Fingerprint;
}

pub(crate) trait Stored: Copy {
    fn offsets(&self) -> &[Offset];
}

pub(crate) trait Suffixed: Stored {
    fn start(&self) -> Offset;
}

impl<const N: usize> Stored for [Offset; N] {
    fn offsets(&self) -> &[Offset] {
        self
    }
}

impl<const N: usize> Proof for [Offset; N] {
    type Retained = Self;

    const FIELDS: usize = N;

    fn offsets(&self) -> &[Offset] {
        self
    }

    fn validate(&self, _cap: usize) -> Result<(), Fault> {
        Ok(())
    }

    fn retained(self) -> Self::Retained {
        self
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Identity<const N: usize> {
    offsets: [Offset; N],
    start: Offset,
    fingerprint: Fingerprint,
}

#[derive(Clone, Copy)]
pub(crate) struct Suffix<const N: usize> {
    offsets: [Offset; N],
    start: Offset,
}

impl<const N: usize> Identity<N> {
    pub(in crate::protocol::wire) const fn new(
        offsets: [Offset; N],
        start: Offset,
        hash: Fingerprint,
    ) -> Self {
        Self {
            offsets,
            start,
            fingerprint: hash,
        }
    }
}

impl<const N: usize> Proof for Identity<N> {
    type Retained = Suffix<N>;

    const FIELDS: usize = N;

    fn offsets(&self) -> &[Offset] {
        &self.offsets
    }

    fn validate(&self, cap: usize) -> Result<(), Fault> {
        if self.start.get() as usize > cap {
            return Err(Fault::EncodedSuffix {
                offset: self.start.get(),
                available: cap,
            });
        }
        Ok(())
    }

    fn retained(self) -> Self::Retained {
        Suffix {
            offsets: self.offsets,
            start: self.start,
        }
    }
}

impl<const N: usize> Identified for Identity<N> {
    fn start(&self) -> Offset {
        self.start
    }

    fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }
}

impl<const N: usize> Stored for Suffix<N> {
    fn offsets(&self) -> &[Offset] {
        &self.offsets
    }
}

impl<const N: usize> Suffixed for Suffix<N> {
    fn start(&self) -> Offset {
        self.start
    }
}

pub(crate) trait Append<T> {
    type Next;

    const LEN: usize;

    fn append(self, value: T) -> Self::Next;
}

impl<T> Append<T> for [T; 0] {
    type Next = [T; 1];

    const LEN: usize = 0;

    fn append(self, value: T) -> Self::Next {
        [value]
    }
}

impl<T> Append<T> for [T; 1] {
    type Next = [T; 2];

    const LEN: usize = 1;

    fn append(self, value: T) -> Self::Next {
        let [first] = self;
        [first, value]
    }
}

impl<T> Append<T> for [T; 2] {
    type Next = [T; 3];

    const LEN: usize = 2;

    fn append(self, value: T) -> Self::Next {
        let [first, second] = self;
        [first, second, value]
    }
}

pub(crate) trait Layout {
    type Proof: Proof;
}

#[derive(Clone, Copy)]
pub(crate) struct Empty;

impl Layout for Empty {
    type Proof = [Offset; 0];
}

const _: () = assert!(std::mem::size_of::<<Empty as Layout>::Proof>() == 0);
const _: () = assert!(std::mem::size_of::<<queues::Declaration as Layout>::Proof>() == 4);
const _: () = assert!(std::mem::size_of::<<exchange::Declare as Layout>::Proof>() == 8);
const _: () = {
    assert!(std::mem::size_of::<<queues::Binding as Layout>::Proof>() == 24);
    assert!(std::mem::size_of::<<<queues::Binding as Layout>::Proof as Proof>::Retained>() == 16);
};
