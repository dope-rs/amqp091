use std::mem;

use o3::depot::arena::pool::recycle::release;

use crate::service::content::entry;
use crate::service::content::wait::core::Store;
use crate::service::content::wait::release::Release;

#[derive(Clone, Copy)]
pub(crate) struct Wake<'b, 'd>(&'b Store<'d>);

impl<'b, 'd> Wake<'b, 'd> {
    pub(super) const fn new(store: &'b Store<'d>) -> Self {
        Self(store)
    }

    pub(in crate::service::content) fn complete(
        &self,
        body: usize,
        fragments: u32,
        resident: bool,
    ) {
        self.0.complete(body, fragments, resident);
    }
}

impl<'c, 'b: 'c, 'd: 'c> release::Context<'c, entry::Entry<'b, 'd>> for Wake<'b, 'd> {
    fn released(&self, release: Release) {
        self.0.released(release);
    }
}

const _: () = {
    assert!(mem::size_of::<Wake<'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Wake<'static, 'static>>>() == mem::size_of::<usize>());
};
