use crate::failure::{self, recovery};
use crate::protocol::wire::text::resident;

pub(crate) enum Terminal {}

impl recovery::Policy for Terminal {
    fn classify<'b, 'd>(
        failure: recovery::Fault<'b, 'd, Self>,
    ) -> (recovery::Class, failure::Error<resident::Short<'b, 'd>>) {
        (recovery::Class::Fatal, failure.into_error())
    }
}
