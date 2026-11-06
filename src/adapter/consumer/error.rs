use crate::failure;

/// Failure to settle one delivery against its exact source generation.

#[derive(Debug)]
pub enum Error<'c, 'b: 'c, 'd: 'b, const ID: u8> {
    /// The source generation or channel is unavailable. No frame was sent.
    SourceUnavailable(failure::Runtime<'c, 'b, 'd, ID>),
    /// The source was active, but frame admission failed.
    Failed(failure::Runtime<'c, 'b, 'd, ID>),
}

impl<'c, 'b: 'c, 'd: 'b, const ID: u8> Error<'c, 'b, 'd, ID> {
    pub(super) fn from_runtime(error: failure::Runtime<'c, 'b, 'd, ID>) -> Self {
        match error {
            failure::Runtime::Terminal(_)
            | failure::Runtime::Local(failure::Error::Closed | failure::Error::Retain(_)) => {
                Self::SourceUnavailable(error)
            }
            _ => Self::Failed(error),
        }
    }
}
