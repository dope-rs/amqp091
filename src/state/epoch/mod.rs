use dope::{core::driver::route, manifold::client::conn};

pub(crate) type Id<'d, const ROUTE: u8> = conn::Id<'d, route::KeyTag<ROUTE>>;
