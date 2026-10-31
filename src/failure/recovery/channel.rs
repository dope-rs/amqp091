use crate::failure::{self, recovery};
use crate::protocol::wire::text::resident;

pub(crate) enum Channel {}

impl recovery::Policy for Channel {
    fn classify<'b, 'd>(
        failure: recovery::Fault<'b, 'd, Self>,
    ) -> (recovery::Class, failure::Error<resident::Short<'b, 'd>>) {
        let class = if failure.error().is_not_found() {
            recovery::Class::Recoverable
        } else {
            recovery::Class::Fatal
        };
        (class, failure.into_error())
    }
}
