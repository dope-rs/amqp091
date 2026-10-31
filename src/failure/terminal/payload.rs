use crate::failure;
use crate::protocol::wire::text::resident;

#[derive(Debug)]
pub(crate) struct Payload<'b, 'd> {
    pub(super) generation: u64,
    pub(super) error: failure::Error<resident::Short<'b, 'd>>,
}
