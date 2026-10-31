use std::{marker, mem};

use crate::failure::{self, recovery};
use crate::protocol::wire::text::resident;

#[repr(transparent)]
pub(crate) struct Fault<'b, 'd, P> {
    error: failure::Error<resident::Short<'b, 'd>>,
    policy: marker::PhantomData<P>,
}

impl<'b, 'd> Fault<'b, 'd, recovery::Session> {
    pub(crate) fn session(error: failure::Error) -> Self {
        Self::new(error.into())
    }
}

impl<'b, 'd> Fault<'b, 'd, recovery::Connection> {
    pub(crate) fn connection(error: failure::Error<resident::Short<'b, 'd>>) -> Self {
        Self::new(error)
    }
}

impl<'b, 'd> Fault<'b, 'd, recovery::Channel> {
    pub(crate) fn channel(error: failure::Error<resident::Short<'b, 'd>>) -> Self {
        Self::new(error)
    }
}

impl<'b, 'd> Fault<'b, 'd, recovery::Terminal> {
    pub(crate) fn terminal(error: failure::Error<resident::Short<'b, 'd>>) -> Self {
        Self::new(error)
    }
}

impl<'b, 'd, P: recovery::Policy> Fault<'b, 'd, P> {
    fn new(error: failure::Error<resident::Short<'b, 'd>>) -> Self {
        Self {
            error,
            policy: marker::PhantomData,
        }
    }

    pub(crate) const fn error(&self) -> &failure::Error<resident::Short<'b, 'd>> {
        &self.error
    }

    pub(crate) fn into_error(self) -> failure::Error<resident::Short<'b, 'd>> {
        self.error
    }
}

const _: () = {
    assert!(
        mem::size_of::<Fault<'static, 'static, recovery::Session>>()
            == mem::size_of::<failure::Error<resident::Short<'static, 'static>>>()
    );
    assert!(
        mem::align_of::<Fault<'static, 'static, recovery::Session>>()
            == mem::align_of::<failure::Error<resident::Short<'static, 'static>>>()
    );
    assert!(
        mem::size_of::<Fault<'static, 'static, recovery::Connection>>()
            == mem::size_of::<failure::Error<resident::Short<'static, 'static>>>()
    );
    assert!(
        mem::align_of::<Fault<'static, 'static, recovery::Connection>>()
            == mem::align_of::<failure::Error<resident::Short<'static, 'static>>>()
    );
    assert!(
        mem::size_of::<Fault<'static, 'static, recovery::Channel>>()
            == mem::size_of::<failure::Error<resident::Short<'static, 'static>>>()
    );
    assert!(
        mem::align_of::<Fault<'static, 'static, recovery::Channel>>()
            == mem::align_of::<failure::Error<resident::Short<'static, 'static>>>()
    );
    assert!(
        mem::size_of::<Fault<'static, 'static, recovery::Terminal>>()
            == mem::size_of::<failure::Error<resident::Short<'static, 'static>>>()
    );
    assert!(
        mem::align_of::<Fault<'static, 'static, recovery::Terminal>>()
            == mem::align_of::<failure::Error<resident::Short<'static, 'static>>>()
    );
};
