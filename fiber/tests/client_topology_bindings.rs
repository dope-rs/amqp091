use std::time::Duration;

use dope_amqp091::config;
use dope_amqp091::message::ack;
use dope_amqp091::message::field::{Table, Value};
use dope_amqp091_fiber::channel::{basic, exchange, queue};
use dope_fiber::abi::Join;
use dope_test::harness::peer::AcceptScript;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Method, put_short, short};
use crate::common::client::*;
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

#[test]
fn keeps_binding_identity() {
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
            for request in std::iter::once(50)
                .chain([20, 20, 50, 20, 20, 50, 50].into_iter().cycle().take(14))
                .chain([20, 20, 50, 20])
            {
                Method::read(&mut first)
                    .expect("default routing operation")
                    .check(1, 50, request).expect("default routing method");
                Method::write(&mut first, 1, 50, request + 1, &[])
                    .expect("default routing reply");
            }
            Method::read(&mut first).expect("processed binding fence")
                .check(1, 50, 30).expect("queue.purge");
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
            let mut routes = [String::new(), String::new()];
            for route in &mut routes {
                let binding = Method::read(&mut second).expect("AMQP method");
                binding.check(root, 50, 20).expect("AMQP method route");
                assert_eq!(
                    binding.1.get(binding.1.len() - 4..),
                    Some(2i32.to_be_bytes().as_slice()),
                    "recovery replayed the binding removed by its exact arguments"
                );
                let (_, offset) = short(binding.payload(), 6).expect("binding queue");
                let (_, offset) = short(binding.payload(), offset).expect("binding exchange");
                *route = short(binding.payload(), offset).expect("binding route").0.to_owned();
                Method::write(&mut second, root, 50, 21, &[]).expect("recovered queue.bind-ok");
            }
            routes.sort_unstable();
            assert_eq!(routes, ["", "q"], "literal empty and queue-name routing are distinct");
            if root != 1 {
                let current = Method::read(&mut second).expect("channel current queue restore");
                current.check(1, 50, 10).expect("AMQP method route");
                assert_eq!(short(current.payload(), 6).expect("current queue name").0, "q");
                second
                    .write_all(&Method::try_encode(1, 50, 11, &queue_ok).expect("AMQP frame fixture"))
                    .expect("current queue.declare-ok");
            }
            Method::read(&mut second).expect("connection close")
                .check(0, 10, 50).expect("no duplicate binding before close");
            Method::write(&mut second, 0, 10, 51, &[]).expect("connection.close-ok");
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
                        channel.queue().unbind("q", "x", "rk", &same2)
                            .await.expect("remove retained explicit route");
                        for (name, route) in [("", ""), ("q", "q")] {
                            channel.queue().bind("", "x", "", &args2)
                                .await.expect("queue-name routing");
                            channel.queue().bind("q", "x", "q", &args2)
                                .await.expect("literal queue-name route");
                            channel.queue().unbind(name, "x", route, &same2)
                                .await.expect("remove both current routing aliases");
                            channel.queue().bind("q", "x", "", &args2)
                                .await.expect("literal empty routing");
                            channel.queue().bind("q", "x", "rk", &args1)
                                .await.expect("both routing aliases released their capacity");
                            channel.queue().unbind("q", "x", "rk", &same1)
                                .await.expect("remove routing capacity probe");
                            channel.queue().unbind("q", "x", "", &same2)
                                .await.expect("remove literal empty route");
                        }
                        channel.queue().bind("", "x", "", &args2)
                            .await.expect("distinct queue-name routing");
                        channel.queue().bind("q", "x", "", &args2)
                            .await.expect("distinct literal empty routing");
                        channel.queue().unbind("q", "x", "", &same2)
                            .await.expect("remove only literal empty routing");
                        channel.queue().bind("q", "x", "", &args2)
                            .await.expect("restore literal empty routing");
                        channel.queue().purge("q").await.expect_err("disconnect after committed bindings");
                        conn.observe().retired(target.epoch()).await.expect("first retirement");
                        let ready = conn.observe().ready().await.expect("complete binding replay");
                        conn.control().close(ready, 200, "done").await.expect("close");
                    })).is_ok(), "AMQP fiber must complete");
                }, recover,
                complete,
            );
            broker.finish().expect("binding arguments broker");
        },
    )
    .expect("binding arguments broker");
}

#[test]
fn passive_declaration_selects_current_binding() {
    AcceptScript::with(
        |acceptor| {
            for recovered in [false, true] {
                let mut stream = acceptor.accept().expect("connection");
                stream
                    .read_wait(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                stream
                    .write_wait(Some(Duration::from_secs(10)))
                    .expect("write timeout");
                Peer::new(&mut stream).open(TUNE).expect("handshake");
                Method::read(&mut stream)
                    .expect("channel open")
                    .check(1, 20, 10)
                    .expect("open route");
                Method::write(&mut stream, 1, 20, 11, &[0; 4]).expect("open-ok");
                if recovered {
                    for _ in 0..2 {
                        let request = Method::read(&mut stream).expect("queue replay");
                        request.check(1, 50, 10).expect("declare replay");
                        let name = queue_name(&request).expect("replay name");
                        stream
                            .write_all(
                                &queue_declare_ok_frame(1, name.as_bytes())
                                    .expect("declare fixture"),
                            )
                            .expect("declare-ok");
                    }
                } else {
                    for (name, passive) in [("q", false), ("p", false), ("q", true)] {
                        let request = Method::read(&mut stream).expect("queue declare");
                        request.check(1, 50, 10).expect("declare route");
                        let (actual, flags) = short(request.payload(), 6).expect("queue field");
                        assert_eq!(actual, name);
                        assert_eq!(request.payload()[flags] & 1, u8::from(passive));
                        stream
                            .write_all(
                                &queue_declare_ok_frame(1, name.as_bytes())
                                    .expect("declare fixture"),
                            )
                            .expect("declare-ok");
                    }
                }
                check_queue_bind_target(
                    &Method::read(&mut stream).expect("queue bind"),
                    if recovered { "q" } else { "" },
                    "rk",
                )
                .expect("passive declaration selected q");
                Method::write(&mut stream, 1, 50, 21, &[]).expect("bind-ok");
                let next = Method::read(&mut stream).expect("completion fence");
                if recovered {
                    next.check(0, 10, 50).expect("connection close");
                    Method::write(&mut stream, 0, 10, 51, &[]).expect("close-ok");
                } else {
                    next.check(1, 50, 30).expect("purge fence");
                }
            }
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default().topology(
                            config::Topology::default()
                                .queues(2)
                                .routes(1)
                                .identities(1),
                        ),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("first ready");
                        let channel = conn.channels().open_in(target, 1).await.expect("channel");
                        let args = Table::new();
                        for name in ["q", "p"] {
                            channel.queue().declare(queue::Declare {
                                queue: name,
                                durable: false,
                                exclusive: false,
                                auto_delete: false,
                                arguments: &args,
                            }).await.expect("declare");
                        }
                        channel.queue().inspect(queue::Inspect {
                            queue: "q",
                            durable: false,
                            exclusive: false,
                            auto_delete: false,
                            arguments: &args,
                        }).await.expect("inspect");
                        channel.queue().bind("", "x", "rk", &args).await.expect("bind");
                        channel.queue().purge("q").await.expect_err("disconnect fence");
                        conn.observe().retired(target.epoch()).await.expect("retired");
                        let ready = conn.observe().ready().await.expect("recovered");
                        conn.control().close(ready, 200, "done").await.expect("close");
                    })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                complete,
            );
            broker.finish().expect("passive current binding broker");
        },
    )
    .expect("passive current binding broker");
}

#[test]
fn passive_external_current_survives_reconnect() {
    AcceptScript::with(
        |acceptor| {
            for recovered in [false, true] {
                let mut stream = acceptor.accept().expect("connection");
                stream
                    .read_wait(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                stream
                    .write_wait(Some(Duration::from_secs(10)))
                    .expect("write timeout");
                Peer::new(&mut stream).open(TUNE).expect("handshake");
                Method::read(&mut stream)
                    .expect("channel open")
                    .check(1, 20, 10)
                    .expect("open route");
                Method::write(&mut stream, 1, 20, 11, &[0; 4]).expect("open-ok");
                if !recovered {
                    let inspect = Method::read(&mut stream).expect("passive declaration");
                    inspect.check(1, 50, 10).expect("declare route");
                    let (name, flags) = short(inspect.payload(), 6).expect("queue field");
                    assert_eq!(name, "external");
                    assert_eq!(inspect.payload()[flags] & 1, 1);
                    stream
                        .write_all(
                            &queue_declare_ok_frame(1, b"external").expect("declare fixture"),
                        )
                        .expect("declare-ok");
                }
                let name = if recovered { "external" } else { "" };
                check_queue_bind_target(&Method::read(&mut stream).expect("bind"), name, name)
                    .expect("current queue routing");
                Method::write(&mut stream, 1, 50, 21, &[]).expect("bind-ok");
                check_consume_target(&Method::read(&mut stream).expect("consume"), name, "c")
                    .expect("current queue consumer");
                Method::write(&mut stream, 1, 60, 21, b"\x01c").expect("consume-ok");
                let purge = Method::read(&mut stream).expect("purge");
                purge.check(1, 50, 30).expect("purge route");
                assert_eq!(queue_name(&purge).expect("purge queue"), name);
                if recovered {
                    Method::write(&mut stream, 1, 50, 31, &[0; 4]).expect("purge-ok");
                    let inspect = Method::read(&mut stream).expect("inspect current");
                    inspect.check(1, 50, 10).expect("inspect route");
                    assert_eq!(queue_name(&inspect).expect("inspect name"), "external");
                    stream
                        .write_all(
                            &queue_declare_ok_frame(1, b"external").expect("declare fixture"),
                        )
                        .expect("declare-ok");
                    let purge = Method::read(&mut stream).expect("live current purge");
                    purge.check(1, 50, 30).expect("purge route");
                    assert!(queue_name(&purge).expect("live queue name").is_empty());
                    Method::write(&mut stream, 1, 50, 31, &[0; 4]).expect("purge-ok");
                    Method::read(&mut stream)
                        .expect("close")
                        .check(0, 10, 50)
                        .expect("close route");
                    Method::write(&mut stream, 0, 10, 51, &[]).expect("close-ok");
                }
            }
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest")
                    .expect("config")
                    .capacity(
                        config::Capacity::default().topology(
                            config::Topology::default()
                                .queues(1)
                                .routes(1)
                                .identities(1),
                        ),
                    ),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open_in(target, 1).await.expect("channel");
                        let args = Table::new();
                        channel.queue().inspect(queue::Inspect {
                            queue: "external",
                            durable: false,
                            exclusive: false,
                            auto_delete: false,
                            arguments: &args,
                        }).await.expect("inspect");
                        channel.queue().bind("", "x", "", &args).await.expect("bind");
                        let consumer = channel.basic().consume(basic::Consume {
                            queue: "",
                            consumer_tag: "c",
                            no_local: false,
                            ack: ack::Manual,
                            exclusive: false,
                            arguments: &args,
                        }).await.expect("consume");
                        channel.queue().purge("").await.expect_err("disconnect fence");
                        conn.observe().retired(target.epoch()).await.expect("retired");
                        let ready = conn.observe().ready().await.expect("recovered");
                        let channel = channel.into_auto();
                        channel.queue().purge("").await.expect("named current purge");
                        channel.queue().inspect(queue::Inspect {
                            queue: "",
                            durable: false,
                            exclusive: false,
                            auto_delete: false,
                            arguments: &args,
                        }).await.expect("inspect current");
                        channel.queue().purge("").await.expect("live current purge");
                        conn.control().close(ready, 200, "done").await.expect("close");
                        drop(consumer);
                    })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                complete,
            );
            broker.finish().expect("external current broker");
        },
    )
    .expect("external current broker");
}
