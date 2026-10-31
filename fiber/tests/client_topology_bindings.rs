use std::convert::Infallible;
use std::io::Write as _;
use std::sync::mpsc;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::config;
use dope_amqp091::message::field::{Table, Value};
use dope_amqp091_fiber::channel::{exchange, queue};
use dope_fiber::abi::Join;
use dope_test::fibers::Yield;
use dope_test::harness::peer::AcceptScript;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Method, put_short, short};
use crate::common::client::*;
pub mod common;

#[test]
fn keeps_binding_identity() {
    let (release, wait) = mpsc::channel();
    let (done, finish) = mpsc::channel();
    AcceptScript::with(
        move |acceptor| {
            let mut first = acceptor.accept().expect("first connection");
            first
                .read_wait(Some(Duration::from_secs(10)))
                .expect("first read timeout");
            first
                .write_wait(Some(Duration::from_secs(10)))
                .expect("first write timeout");
            Peer::new(&mut first)
                .open(TUNE)
                .expect("AMQP broker handshake");

            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            first
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("channel.open-ok");
            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 40, 10).expect("AMQP method route");
            first
                .write_all(&Method::try_encode(1, 40, 11, &[]).expect("AMQP frame fixture"))
                .expect("exchange.declare-ok");
            Method::read(&mut first).expect("external queue binding")
                .check(1, 50, 20).expect("queue.bind before declaration");
            Method::write(&mut first, 1, 50, 21, &[]).expect("external queue.bind-ok");
            Method::read(&mut first)
                .expect("second channel open")
                .check(2, 20, 10).expect("AMQP method route");
            first.write_all(
                &Method::try_encode(2, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture")
            ).expect("second channel.open-ok");
            for channel in [1, 2] {
                Method::read(&mut first)
                    .expect("concurrent queue declare")
                    .check(channel, 50, 10).expect("AMQP method route");
            }
            let mut queue_ok = Vec::new();
            put_short(&mut queue_ok, b"q").expect("AMQP short string");
            queue_ok.extend_from_slice(&0u32.to_be_bytes());
            queue_ok.extend_from_slice(&0u32.to_be_bytes());
            for channel in [1, 2] {
                first
                    .write_all(&Method::try_encode(channel, 50, 11, &queue_ok).expect("AMQP frame fixture"))
                    .expect("queue.declare-ok");
            }
            for (request, response, name) in [(20, 21, "q"), (50, 51, "")] {
                let operation = Method::read(&mut first).expect("declared binding operation");
                operation.check(1, 50, request).expect("declared binding route");
                assert_eq!(queue_name(&operation).expect("queue name"), name);
                Method::write(&mut first, 1, 50, response, &[]).expect("declared binding reply");
            }
            Method::read(&mut first)
                .expect("released queue reservation")
                .check(2, 50, 10).expect("AMQP method route");
            first
                .write_all(&queue_declare_ok_frame(2, b"spare").expect("queue reply fixture"))
                .expect("spare queue.declare-ok");
            Method::read(&mut first)
                .expect("spare queue delete")
                .check(2, 50, 40).expect("AMQP method route");
            first
                .write_all(&Method::try_encode(2, 50, 41, &0u32.to_be_bytes()).expect("AMQP frame fixture"))
                .expect("spare queue.delete-ok");

            for _ in 0..2 {
                Method::read(&mut first)
                    .expect("AMQP method")
                    .check(1, 50, 20).expect("AMQP method route");
                first
                    .write_all(&Method::try_encode(1, 50, 21, &[]).expect("AMQP frame fixture"))
                    .expect("queue.bind-ok");
            }
            Method::read(&mut first)
                .expect("queue unbind")
                .check(1, 50, 50).expect("AMQP method route");
            Method::read(&mut first)
                .expect("concurrent queue bind")
                .check(2, 50, 20).expect("AMQP method route");
            first
                .write_all(&Method::try_encode(1, 50, 51, &[]).expect("AMQP frame fixture"))
                .expect("queue.unbind-ok");
            first
                .write_all(&Method::try_encode(2, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("moved binding redeclare-ok");

            Method::read(&mut first)
                .expect("remove retained binding")
                .check(1, 50, 50).expect("AMQP method route");
            first
                .write_all(&Method::try_encode(1, 50, 51, &[]).expect("AMQP frame fixture"))
                .expect("queue.unbind-ok");
            for channel in [1, 2] {
                Method::read(&mut first)
                    .expect("concurrent new binding")
                    .check(channel, 50, 20).expect("AMQP method route");
            }
            for channel in [1, 2] {
                first
                    .write_all(&Method::try_encode(channel, 50, 21, &[]).expect("AMQP frame fixture"))
                    .expect("new binding acknowledge");
            }
            for (request, response) in [(20, 21), (50, 51)] {
                Method::read(&mut first)
                    .expect("released reservation capacity")
                    .check(1, 50, request).expect("AMQP method route");
                first
                    .write_all(&Method::try_encode(1, 50, response, &[]).expect("AMQP frame fixture"))
                    .expect("released reservation response");
            }
            wait.recv().expect("processed queue.unbind-ok");
            drop(first);

            let mut second = acceptor.accept().expect("recovered connection");
            second
                .read_wait(Some(Duration::from_secs(10)))
                .expect("second read timeout");
            second
                .write_wait(Some(Duration::from_secs(10)))
                .expect("second write timeout");
            Peer::new(&mut second)
                .open(TUNE)
                .expect("AMQP broker handshake");
            let mut opened = [0; 2];
            for channel in &mut opened {
                let open = Method::read(&mut second).expect("recovered channel open");
                *channel = open.channel();
                open.check(*channel, 20, 10).expect("AMQP method route");
                second
                    .write_all(
                        &Method::try_encode(*channel, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                    )
                    .expect("recovered channel.open-ok");
            }
            opened.sort_unstable();
            assert_eq!(opened, [1, 2], "recovery must reopen each exact channel once");
            let exchange = Method::read(&mut second).expect("recovered exchange declare");
            let root = exchange.channel();
            assert!(opened.contains(&root), "replay must use a reopened channel");
            exchange.check(root, 40, 10).expect("AMQP method route");
            second
                .write_all(&Method::try_encode(root, 40, 11, &[]).expect("AMQP frame fixture"))
                .expect("recovered exchange.declare-ok");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(root, 50, 10).expect("AMQP method route");
            second
                .write_all(&Method::try_encode(root, 50, 11, &queue_ok).expect("AMQP frame fixture"))
                .expect("recovered queue.declare-ok");
            let binding = Method::read(&mut second).expect("AMQP method");
            binding.check(root, 50, 20).expect("AMQP method route");
            assert_eq!(
                binding.1.get(binding.1.len() - 4..),
                Some(2i32.to_be_bytes().as_slice()),
                "recovery replayed the binding removed by its exact arguments"
            );
            second
                .write_all(&Method::try_encode(root, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("recovered queue.bind-ok");
            if root != 1 {
                let current = Method::read(&mut second).expect("channel current queue restore");
                current.check(1, 50, 10).expect("AMQP method route");
                assert_eq!(short(current.payload(), 6).expect("current queue name").0, "q");
                second
                    .write_all(&Method::try_encode(1, 50, 11, &queue_ok).expect("AMQP frame fixture"))
                    .expect("current queue.declare-ok");
            }
            check_no_frame(&second, "duplicate binding replay").expect("AMQP fixture");
            done.send(()).expect("publish binding identity replay");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default().topology(config::Topology::default().routes(2).queues(2)),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("first ready");
                        let epoch = conn.observe().read(|view| view.generation()).await;
                        let channel = conn
                            .channels()
                            .open_in(target, 1)
                            .await
                            .expect("channel open");
                        let empty = Table::new();
                        channel
                            .exchange()
                            .declare(exchange::Declare {
                                exchange: "x",
                                kind: "direct",
                                durable: false,
                                auto_delete: false,
                                internal: false,
                                arguments: &empty,
                            })
                            .await
                            .expect("exchange declare");
                        channel.queue().bind("q", "x", "external", &empty)
                            .await.expect("binding before local queue declaration");
                        let second = conn
                            .channels()
                            .open_in(target, 2)
                            .await
                            .expect("second channel open");
                        let options = queue::Declare {
                            queue: "q",
                            durable: false,
                            exclusive: false,
                            auto_delete: false,
                            arguments: &empty,
                        };
                        let (first, other) = Join::new(
                            channel.queue().declare(queue::Declare { ..options }),
                            second.queue().declare(queue::Declare { ..options }),
                        ).await;
                        first.expect("first concurrent queue");
                        other.expect("coalesced concurrent queue");
                        channel.queue().bind("q", "x", "external", &empty)
                            .await.expect("binding with declared queue identity");
                        channel.queue().unbind("", "x", "external", &empty)
                            .await.expect("remove name and identity bindings");
                        second.queue().declare(queue::Declare { queue: "spare", ..options })
                            .await.expect("released queue reservation");
                        second.queue().delete("spare", false, false)
                            .await.expect("remove capacity probe queue");
                        let mut args1 = Table::new();
                        args1.insert("z-id", Value::from(1i32)).expect("first id");
                        args1.insert("a-kind", Value::from(true)).expect("first kind");
                        let mut same1 = Table::new();
                        same1
                            .insert("a-kind", Value::from(true))
                            .expect("reordered first kind");
                        same1
                            .insert("z-id", Value::from(1i32))
                            .expect("reordered first id");
                        let mut args2 = Table::new();
                        args2
                            .insert("a-kind", Value::from(true))
                            .expect("second kind");
                        args2
                            .insert("z-id", Value::from(2i32))
                            .expect("second id");
                        let mut same2 = Table::new();
                        same2
                            .insert("z-id", Value::from(2i32))
                            .expect("reordered second id");
                        same2
                            .insert("a-kind", Value::from(true))
                            .expect("reordered second kind");
                        channel
                            .queue()
                            .bind("q", "x", "rk", &args1)
                            .await
                            .expect("first binding");
                        channel
                            .queue()
                            .bind("q", "x", "rk", &args2)
                            .await
                            .expect("second binding");
                        let (removed, moved) = Join::new(
                            channel.queue().unbind("q", "x", "rk", &same1),
                            second.queue().bind("q", "x", "rk", &same2),
                        ).await;
                        removed.expect("exact unbind");
                        moved.expect("moved binding redeclare");
                        channel.queue().unbind("q", "x", "rk", &same2)
                            .await.expect("remove retained binding");
                        let (first, other) = Join::new(
                            channel.queue().bind("q", "x", "rk", &args2),
                            second.queue().bind("q", "x", "rk", &same2),
                        ).await;
                        first.expect("first concurrent binding");
                        other.expect("coalesced concurrent binding");
                        channel.queue().bind("q", "x", "rk", &args1)
                            .await.expect("released reservation capacity");
                        channel.queue().unbind("q", "x", "rk", &same1)
                            .await.expect("remove capacity probe binding");
                        release.send(()).expect("publish processed queue.unbind-ok");

                        while conn.observe().read(|view| view.generation()).await == epoch {
                            Yield::new().await;
                        }
                        loop {
                            match finish.try_recv() { Ok(()) => break, Err(mpsc::TryRecvError::Empty) => Yield::new().await, outcome => outcome.expect("broker ended before binding identity replay"), }
                        }
                    })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("endpoint completion");
                },
            );
            broker.finish().expect("binding arguments broker");
        },
    )
    .expect("binding arguments broker");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
