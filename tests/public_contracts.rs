//! Public ownership, lifetime, and construction contracts.
//!
//! ```
//! use dope::core::driver;
//! use dope::core::driver::schedule::ready::wake::{self, queue};
//! use dope_amqp091::adapter::{Adapter, channel::Lease, conn::{Epoch, Ready}};
//! use dope_amqp091::protocol::method::channel::CloseOk;
//! use dope_amqp091::service::{Build, Port, Spec};
//! use o3::base::cell::{local, region};
//!
//! fn same_scope<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b>(
//!     left: Adapter<'o, 'r, 'c, 'b, 'd, 0>,
//!     right: Adapter<'o, 'r, 'c, 'b, 'd, 0>,
//!     mut access: local::Access<'_, region::Identity<'d>>,
//!     target: Epoch<'d, 0>, lease: Lease<'c, 'd, 0>, wakes: [wake::Waker<'d>; 2],
//! ) {
//!     let [first, second] = wakes;
//!     let mut credit = queue::Registration::new();
//!     let _ = left.channels().poll_send(access.reborrow(), target, lease, &CloseOk, &mut credit, first);
//!     credit.clear();
//!     let _ = right.channels().poll_send(access, target, lease, &CloseOk, &mut credit, second);
//! }
//!
//! fn same_port<'d>(spec: Spec, driver: &mut driver::Context<'_, 'd>, target: Ready<'d, 0>)
//!     -> Result<(), Build>
//! {
//!     Port::try_with(spec, driver, |driver| driver.reborrow(), |port, driver| {
//!         let left = Adapter::new(&port);
//!         let right = left;
//!         if let Ok(reservation) = left.channels().reserve(local::Access::new(driver.region_token()), target, 1) {
//!             let lease = reservation.commit();
//!             let _ = right.channels().flow_active(local::Access::new(driver.region_token()), target, lease);
//!         }
//!     })
//! }
//! ```
//!
//! ```compile_fail
//! use dope::core::driver::schedule::ready::wake::{self, queue};
//! use dope_amqp091::adapter::{Adapter, channel::Lease, conn::Epoch};
//! use dope_amqp091::protocol::method::channel::CloseOk;
//! use o3::base::cell::{local, region};
//!
//! fn distinct_scopes<'o, 'r: 'o, 'left: 'r, 'right: 'r, 'b: 'left + 'right, 'd: 'b>(
//!     left: Adapter<'o, 'r, 'left, 'b, 'd, 0>,
//!     right: Adapter<'o, 'r, 'right, 'b, 'd, 0>,
//!     mut access: local::Access<'_, region::Identity<'d>>,
//!     target: Epoch<'d, 0>, lease: Lease<'left, 'd, 0>, wakes: [wake::Waker<'d>; 2],
//! ) {
//!     let [first, second] = wakes;
//!     let mut credit = queue::Registration::new();
//!     let _ = left.channels().poll_send(access.reborrow(), target, lease, &CloseOk, &mut credit, first);
//!     credit.clear();
//!     let _ = right.channels().poll_send(access, target, lease, &CloseOk, &mut credit, second);
//! }
//! ```
//!
//! ```compile_fail
//! use dope::core::driver;
//! use dope_amqp091::{adapter::{Adapter, conn::Ready}, service::{Build, Port, Spec}};
//! use o3::base::cell::local;
//!
//! fn cross_port<'d>(first: Spec, second: Spec, driver: &mut driver::Context<'_, 'd>, target: Ready<'d, 0>)
//!     -> Result<(), Build>
//! {
//!     Port::try_with(first, driver, |driver| driver.reborrow(), |outer, driver| {
//!         let Some(lease) = Adapter::new(&outer).channels().current(local::Access::new(driver.region_token()), 1) else {
//!             return;
//!         };
//!         let _ = Port::try_with(second, driver, |driver| driver.reborrow(), |inner, driver| {
//!             let _ = Adapter::new(&inner).channels().flow_active(local::Access::new(driver.region_token()), target, lease);
//!         });
//!     })
//! }
//! ```
//!
//! ```compile_fail,E0435
//! use dope_amqp091::protocol::wire::text::short;
//!
//! fn dynamic(value: &str) -> short::Short<'_> {
//!     short::literal!(short::Short::try_new(value))
//! }
//! ```
//!
//! ```compile_fail,E0080
//! use dope_amqp091::protocol::wire::text::short;
//! const _: short::Short<'static> = short::literal!(short::Short::try_new("queue\0suffix"));
//! ```
//!
//! ```compile_fail,E0080
//! use dope_amqp091::protocol::wire::text::short;
//! const _: short::Short<'static> = short::literal!(short::Short::try_new(concat!(
//!     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
//!     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
//!     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
//!     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
//! )));
//! ```
//!
//! ```compile_fail,E0616
//! use dope_amqp091::message::property::Properties;
//!
//! fn forge(properties: &mut Properties) {
//!     properties.delivery_mode = std::num::NonZero::new(3);
//! }
//! ```
//!
//! ```compile_fail,E0616
//! use dope_amqp091::message::property::Properties;
//!
//! fn forge(properties: &mut Properties) {
//!     properties.priority = Some(10);
//! }
//! ```
//!
//!
//! ```compile_fail,E0308
//! use dope_amqp091::adapter::consumer;
//! use dope_amqp091::message::ack;
//! use dope_amqp091::protocol::ConsumerKey;
//!
//! fn rebrand<'d>(
//!     key: ConsumerKey<consumer::Key<'d, 'd, 0>, ack::Auto>,
//! ) -> ConsumerKey<consumer::Key<'d, 'd, 0>, ack::Manual> {
//!     key
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope::core::driver;
//! use dope_amqp091::{adapter::Adapter, service::{Build, Port, Spec}};
//! fn escape<'d>(spec: Spec, driver: &mut driver::Context<'_, 'd>)
//!     -> Result<Adapter<'d, 'd, 'd, 'd, 'd, 0>, Build>
//! {
//!     Port::try_with(spec, driver, |driver| driver.reborrow(), |port, _| {
//!         Adapter::new(&port)
//!     })
//! }
//! ```
//!
//! ```compile_fail
//! use dope::core::driver;
//! use dope_amqp091::{adapter::{Adapter, channel::Lease}, service::{Build, Port, Spec}};
//! use o3::base::cell::local;
//!
//! fn escape<'d>(spec: Spec, driver: &mut driver::Context<'_, 'd>)
//!     -> Result<Option<Lease<'d, 'd, 0>>, Build>
//! {
//!     Port::try_with(spec, driver, |driver| driver.reborrow(), |port, driver| {
//!         Adapter::new(&port).channels().current(local::Access::new(driver.region_token()), 1)
//!     })
//! }
//! ```
//!
//! ```compile_fail
//! use dope::core::driver;
//! use dope_amqp091::{adapter::{Adapter, channel::Reservation, conn::Ready}, service::{Build, Port, Spec}};
//! use o3::base::cell::local;
//!
//! fn escape<'d>(spec: Spec, driver: &mut driver::Context<'_, 'd>, target: Ready<'d, 0>)
//!     -> Result<Option<Reservation<'d, 'd, 'd, 0>>, Build>
//! {
//!     Port::try_with(spec, driver, |driver| driver.reborrow(), |port, driver| {
//!         Adapter::new(&port).channels().reserve(local::Access::new(driver.region_token()), target, 1).ok()
//!     })
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091::adapter::consumer::Delivery;
//! use dope_amqp091::message::content::Properties;
//!
//! fn detach<'a, 'd>(delivery: &'a Delivery<'_, '_, '_, '_, 'd, 0>) -> Properties<'static, 'd> {
//!     delivery.properties()
//! }
//! ```
//!
//!
//! ```
//! use dope_amqp091::adapter::consumer::Delivery;
//! use dope_amqp091::message::content::Properties;
//!
//! fn borrow<'a, 'd>(delivery: &'a Delivery<'_, '_, '_, '_, 'd, 0>) -> Properties<'a, 'd> {
//!     delivery.properties()
//! }
//!
//! fn content_type<'a, 'd>(properties: Properties<'a, 'd>) -> Option<&'a str> {
//!     properties.as_ref().content_type()
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091::state::shared::Session;
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091::adapter::Adapter;
//! fn inject_raw<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b, const ID: u8>(adapter: Adapter<'o, 'r, 'c, 'b, 'd, ID>) {
//!     adapter.egress();
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091::adapter::Adapter;
//! fn widen<'o, 'r: 'o, 'c: 'r, 'b: 'c, 'short: 'b, 'long: 'short, const ID: u8>(
//!     adapter: Adapter<'o, 'r, 'c, 'b, 'short, ID>,
//! ) -> Adapter<'o, 'r, 'c, 'b, 'long, ID>
//! where
//!     'long: 'short,
//! {
//!     adapter
//! }
//! ```
//!
//!
//! ```compile_fail,E0308
//! use dope_amqp091::adapter::channel::Lease;
//!
//! fn cross_route<'c, 'd>(lease: Lease<'c, 'd, 1>) -> Lease<'c, 'd, 2> {
//!     lease
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091::adapter::channel::Lease;
//!
//! fn widen<'c, 'short, 'long>(lease: Lease<'c, 'short, 0>) -> Lease<'c, 'long, 0>
//! where
//!     'long: 'short,
//! {
//!     lease
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091::adapter::conn::Epoch;
//! fn widen<'a, 'b>(value: Epoch<'a, 0>) -> Epoch<'b, 0> where 'b: 'a { value }
//! ```
//!
//! ```compile_fail,E0277
//! use dope::net::link::egress::data;
//! use dope_amqp091::protocol::wire::Writer;
//! fn submit<'d>(value: impl data::raw::Payload<'d>) {}
//! fn unfinished(writer: Writer) {
//!     submit(writer);
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use o3::buffer::pool;
//! use dope_amqp091::protocol::wire::Writer;
//! fn unchecked(writer: Writer) -> pool::Cursor {
//!     writer.try_finish()
//! }
//! ```

#[test]
fn checks_public_contracts() -> Result<(), dope_test::checks::Error> {
    use dope_test::checks::Suite;

    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["dope-amqp091", "dope", "o3"],
    );
    let exe = std::env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let suite = suite.try_collect(&exe, output)?;
    assert_eq!(suite.try_errors(include_str!("public_contracts.rs"))?, 9);
    let status = suite.try_rustdoc()?.status()?;
    assert!(status.success(), "public contracts: {status}");
    Ok(())
}
