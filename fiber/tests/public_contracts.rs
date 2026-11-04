//! Public ownership, lifetime, and construction contracts.
//!
//!
//! ```compile_fail,E0308
//! use dope_amqp091_fiber::channel::consumer::Delivery;
//!
//! fn cross_route<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b>(delivery: Delivery<'app, 'o, 'r, 'c, 'b, 'd, (), 1>)
//!     -> Delivery<'app, 'o, 'r, 'c, 'b, 'd, (), 2>
//! {
//!     delivery
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091_fiber::channel::consumer::Delivery;
//!
//! fn widen<'short, 'long: 'short, 'o: 'long, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b>(delivery: Delivery<'short, 'o, 'r, 'c, 'b, 'd, (), 0>)
//!     -> Delivery<'long, 'o, 'r, 'c, 'b, 'd, (), 0>
//! where
//!     'd: 'long,
//!     'long: 'short,
//! {
//!     delivery
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091::message::ack;
//! use dope_amqp091_fiber::channel::consumer::Delivery;
//!
//! fn settle(delivery: Delivery<'static, 'static, 'static, 'static, 'static, 'static, (), 0, ack::Auto>) {
//!     delivery.settle(ack::Disposition::Ack);
//! }
//! ```
//!
//!
//! ```compile_fail,E0308
//! use dope_amqp091_fiber::conn::Lease;
//!
//! fn cross_route<'d>(lease: Lease<'d, 1>) -> Lease<'d, 2> {
//!     lease
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091_fiber::conn::Lease;
//!
//! fn widen<'short, 'long>(lease: Lease<'short, 0>) -> Lease<'long, 0>
//! where
//!     'long: 'short,
//! {
//!     lease
//! }
//! ```
//!
//!
//! ```compile_fail,E0308
//! use dope_amqp091_fiber::conn::Connection;
//!
//! fn change_route<'app, 'o: 'app, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b>(
//!     connection: Connection<'app, 'o, 'r, 'c, 'b, 'd, (), 0>,
//! ) -> Connection<'app, 'o, 'r, 'c, 'b, 'd, (), 1> {
//!     connection
//! }
//! ```
//!
//!
//! ```compile_fail
//! use dope_amqp091_fiber::conn::Connection;
//!
//! fn widen<'short, 'long: 'short, 'o: 'long, 'r: 'o, 'c: 'r, 'b: 'c, 'd: 'b>(
//!     connection: Connection<'short, 'o, 'r, 'c, 'b, 'd, (), 0>,
//! ) -> Connection<'long, 'o, 'r, 'c, 'b, 'd, (), 0>
//! where
//!     'd: 'long,
//!     'long: 'short,
//! {
//!     connection
//! }
//! ```

#[test]
fn checks_public_contracts() -> Result<(), dope_test::checks::Error> {
    use dope_test::checks::Suite;

    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "dope-amqp091",
            "dope-amqp091-fiber",
            "dope",
            "dope-fiber",
            "o3",
        ],
    );
    let exe = std::env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;
    assert_eq!(
        suite.try_errors(include_str!("public_contracts.rs"), compile)?,
        3
    );
    let status = suite.try_rustdoc()?.status().expect("run rustdoc");
    assert!(status.success(), "public contracts: {status}");
    Ok(())
}

fn compile(command: &mut std::process::Command, source: &str) -> std::process::Output {
    let mut child = command.spawn().expect("start compiler");
    let written = child
        .stdin
        .take()
        .map(|mut input| std::io::Write::write_all(&mut input, source.as_bytes()));
    let output = child.wait_with_output().expect("reap compiler");
    written
        .expect("compiler stdin")
        .expect("write compiler source");
    output
}
