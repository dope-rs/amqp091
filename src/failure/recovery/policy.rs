use crate::failure::{self, recovery};
use crate::protocol::wire::text::resident;

pub(crate) trait Policy: Sized {
    fn classify<'b, 'd>(
        failure: recovery::Fault<'b, 'd, Self>,
    ) -> (recovery::Class, failure::Error<resident::Short<'b, 'd>>);
}
