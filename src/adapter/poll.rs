use std::task;

use dope::core::driver::schedule::ready::wake;
use o3::base::cell::{local, region};

use crate::failure;

/// Polls an operation while retaining its original driver and owner lifetimes.
pub trait Poll<'c, 'b: 'c, 'd: 'b, const ID: u8> {
    type Output;

    fn poll(
        &mut self,
        access: local::Access<'_, region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<Self::Output, failure::Runtime<'c, 'b, 'd, ID>>>;
}
