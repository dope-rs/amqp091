use std::convert::Infallible;
use std::time::Duration;

use dope::core::driver::life::recovery;
use dope_amqp091::failure::wire::Fault;
use dope_amqp091::message::field::Table;
use dope_amqp091::{config, failure};
use dope_amqp091_fiber::channel::{exchange, queue};
use dope_fiber::abi::Join;

use crate::common::amqp::Peer;
use crate::common::amqp::wire::{Method, put_short};
use crate::common::fiber::{Pinned, poll_once};
pub mod common;

use std::sync::mpsc;

use dope_test::fibers::Yield;
use dope_test::harness::peer::{AcceptScript, TcpScript};

use crate::common::client::*;

#[test]
fn commits_dropped_ops() {
    let (seen, events) = mpsc::channel();
    let (next, steps) = mpsc::channel();
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
            seen.send(()).expect("publish exchange request");
            steps.recv().expect("dropped exchange request");
            first
                .write_all(&Method::try_encode(1, 40, 11, &[]).expect("AMQP frame fixture"))
                .expect("exchange.declare-ok");

            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 50, 10).expect("AMQP method route");
            assert!(seen.send(()).is_ok(), "publish queue request");
            assert!(steps.recv().is_ok(), "dropped queue request");
            let mut queue_ok = Vec::new();
            put_short(&mut queue_ok, b"q").expect("AMQP short string");
            queue_ok.extend_from_slice(&0u32.to_be_bytes());
            queue_ok.extend_from_slice(&0u32.to_be_bytes());
            first
                .write_all(&Method::try_encode(1, 50, 11, &queue_ok).expect("AMQP frame fixture"))
                .expect("queue.declare-ok");
            first
                .write_all(
                    &Method::try_encode(0, 10, 60, &[5, b'q', b'u', b'e', b'u', b'e'])
                        .expect("AMQP frame fixture"),
                )
                .expect("queue response barrier");

            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 50, 20).expect("AMQP method route");
            first
                .write_all(&Method::try_encode(1, 50, 21, &[]).expect("AMQP frame fixture"))
                .expect("queue.bind-ok");

            Method::read(&mut first)
                .expect("AMQP method")
                .check(1, 50, 50).expect("AMQP method route");
            seen.send(()).expect("publish unbind request");
            steps.recv().expect("dropped unbind request");
            first
                .write_all(&Method::try_encode(1, 50, 51, &[]).expect("AMQP frame fixture"))
                .expect("queue.unbind-ok");
            first
                .write_all(&Method::try_encode(0, 10, 61, &[]).expect("AMQP frame fixture"))
                .expect("unbind response barrier");
            steps.recv().expect("release first topology connection");
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

            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 20, 10).expect("AMQP method route");
            second
                .write_all(
                    &Method::try_encode(1, 20, 11, &[0, 0, 0, 0]).expect("AMQP frame fixture"),
                )
                .expect("recovered channel.open-ok");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 40, 10).expect("AMQP method route");
            second
                .write_all(&Method::try_encode(1, 40, 11, &[]).expect("AMQP frame fixture"))
                .expect("recovered exchange.declare-ok");
            Method::read(&mut second)
                .expect("AMQP method")
                .check(1, 50, 10).expect("AMQP method route");
            second
                .write_all(&Method::try_encode(1, 50, 11, &queue_ok).expect("AMQP frame fixture"))
                .expect("recovered queue.declare-ok");
            check_no_frame(&second, "removed queue binding replay").expect("AMQP fixture");
            seen.send(()).expect("publish topology replay observation");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config"),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("first ready");
                        let epoch = conn.observe().read(|view| view.generation()).await;
                        let channel = conn
                            .channels().open_in(target, 1)
                            .await
                            .expect("first channel open");
                        let args = Table::new();

                        let mut op = Pinned::new(channel.exchange().declare(
                            exchange::Declare {
                                exchange: "x",
                                kind: "direct",
                                durable: false,
                                auto_delete: false,
                                internal: false,
                                arguments: &args,
                            },
                        ));
                        assert!(poll_once(op.as_pin()).await.is_pending());
                        loop {
                            match events.try_recv() {
                                Ok(()) => break,
                                Err(mpsc::TryRecvError::Empty) => {
                                    assert!(poll_once(op.as_pin()).await.is_pending());
                                    Yield::new().await;
                                }
                                disconnected => { disconnected.expect("broker ended before exchange request observation"); }
                            }
                        }
                        drop(op);
                        next.send(()).expect("publish dropped exchange request");

                        let mut op = Pinned::new(channel.queue().declare(queue::Declare {
                            queue: "q",
                            durable: false,
                            exclusive: false,
                            auto_delete: false,
                            arguments: &args,
                        }));
                        loop {
                            match events.try_recv() {
                                Ok(()) => break,
                                Err(mpsc::TryRecvError::Empty) => {
                                    assert!(poll_once(op.as_pin()).await.is_pending());
                                    Yield::new().await;
                                }
                                disconnected => { disconnected.expect("broker ended before queue request observation"); }
                            }
                        }
                        drop(op);
                        assert!(next.send(()).is_ok(), "publish dropped queue request");
                        while !conn.observe().read(|view| view.is_blocked()).await {
                            Yield::new().await;
                        }

                        channel
                            .queue()
                            .bind("q", "x", "rk", &args)
                            .await
                            .expect("queue bind");

                        let mut op = Pinned::new(channel.queue().unbind("q", "x", "rk", &args));
                        assert!(poll_once(op.as_pin()).await.is_pending());
                        loop {
                            match events.try_recv() {
                                Ok(()) => break,
                                Err(mpsc::TryRecvError::Empty) => {
                                    assert!(poll_once(op.as_pin()).await.is_pending());
                                    Yield::new().await;
                                }
                                disconnected => { disconnected.expect("broker ended before unbind request observation"); }
                            }
                        }
                        drop(op);
                        next.send(()).expect("publish dropped unbind request");
                        while conn.observe().read(|view| view.is_blocked()).await {
                            Yield::new().await;
                        }
                        next.send(()).expect("release first topology connection");

                        while conn.observe().read(|view| view.generation()).await == epoch {
                            Yield::new().await;
                        }
                        loop {
                            match events.try_recv() { Ok(()) => break, Err(mpsc::TryRecvError::Empty) => Yield::new().await, outcome => outcome.expect("broker ended before topology replay observation"), }
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
            broker.finish().expect("topology intent broker");
        },
    )
    .expect("topology intent broker");
}

#[test]
fn rejects_deleted_binding() {
    let (seen, events) = mpsc::channel();
    TcpScript::with(
        move |stream| {
            Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
            for channel in [1, 2] {
                Method::read(stream)
                    .expect("channel open")
                    .check(channel, 20, 10).expect("AMQP method route");
                Method::write(stream, channel, 20, 11, &[0, 0, 0, 0])
                    .expect("channel.open-ok");
            }
            Method::read(stream)
                .expect("queue declare")
                .check(1, 50, 10).expect("AMQP method route");
            stream.write_all(&queue_declare_ok_frame(1, b"q").expect("queue reply fixture"))
                .expect("queue.declare-ok");
            Method::read(stream)
                .expect("old generation bind")
                .check(1, 50, 20).expect("AMQP method route");
            seen.send(()).expect("publish old generation bind");
            Method::read(stream)
                .expect("concurrent delete")
                .check(2, 50, 40).expect("AMQP method route");
            Method::write(stream, 2, 50, 41, &0u32.to_be_bytes())
                .expect("queue.delete-ok");
            Method::read(stream)
                .expect("replacement generation declare")
                .check(2, 50, 10).expect("AMQP method route");
            stream.write_all(&queue_declare_ok_frame(2, b"q").expect("queue reply fixture"))
                .expect("replacement queue.declare-ok");
            Method::write(stream, 1, 50, 21, &[]).expect("stale queue.bind-ok");
            Method::read(stream)
                .expect("stale generation protocol close")
                .check(0, 10, 50).expect("AMQP method route");
            Method::write(stream, 0, 10, 51, &[]).expect("connection.close-ok");
        },
        |broker| {
            with(
                broker.addr(),
                config::Config::new("guest", "guest").expect("config")
                    .capacity(config::Capacity::default().topology(config::Topology::default().queues(1))),
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                        let target = conn.observe().ready().await.expect("ready");
                        let channel = conn.channels().open_in(target, 1).await.expect("first channel");
                        let second = conn.channels().open_in(target, 2).await.expect("second channel");
                        let args = Table::new();
                        let options = queue::Declare {
                            queue: "q", durable: false, exclusive: false,
                            auto_delete: false, arguments: &args,
                        };
                        channel.queue().declare(queue::Declare { ..options })
                            .await.expect("original queue");
                        let mut bind = Pinned::new(channel.queue().bind("q", "amq.direct", "rk", &args));
                        loop {
                            match events.try_recv() {
                                Ok(()) => break,
                                Err(mpsc::TryRecvError::Empty) => {
                                    assert!(poll_once(bind.as_pin()).await.is_pending());
                                    Yield::new().await;
                                }
                                result => { result.expect("broker ended before old generation bind"); }
                            }
                        }
                        second.queue().delete("q", false, false)
                            .await.expect("delete original queue");
                        second.queue().declare(options)
                            .await.expect("reuse queue slot");
                        let bound = bind.await;
                        assert!(matches!(bound, Err(ref error) if matches!(
                            error.error(), failure::Error::Protocol(Fault::TopologyQueue { .. })
                        )), "stale binding must preserve its generation error");
                    })).is_ok(), "AMQP fiber must complete");
                }, recover,
                |result| {
                    result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                },
            );
            broker.finish().expect("stale binding broker");
        },
    ).expect("stale binding broker");
}

#[test]
fn rejects_deleted_declare() {
    for retain in [true, false] {
        let (seen, events) = mpsc::channel();
        TcpScript::with(
            move |stream| {
                Peer::new(stream).open(TUNE).expect("AMQP broker handshake");
                for channel in [1, 2] {
                    Method::read(stream)
                        .expect("channel open")
                        .check(channel, 20, 10).expect("AMQP method route");
                    Method::write(stream, channel, 20, 11, &[0, 0, 0, 0])
                        .expect("channel.open-ok");
                }
                let initial = Method::read(stream).expect("anonymous queue declare");
                initial.check(1, 50, 10).expect("AMQP method route");
                assert_eq!(queue_name(&initial).expect("queue name"), "");
                stream.write_all(&queue_declare_ok_frame(1, b"q").expect("queue reply fixture"))
                    .expect("queue.declare-ok");

                let delete = Method::read(stream).expect("concurrent queue delete");
                delete.check(2, 50, 40).expect("AMQP method route");
                assert_eq!(queue_name(&delete).expect("queue name"), "q");
                seen.send(()).expect("publish queue delete");
                let declare = Method::read(stream).expect("named queue redeclare");
                declare.check(1, 50, 10).expect("AMQP method route");
                assert_eq!(queue_name(&declare).expect("queue name"), "q");
                Method::write(stream, 2, 50, 41, &0u32.to_be_bytes())
                    .expect("queue.delete-ok");
                stream.write_all(&queue_declare_ok_frame(1, b"q").expect("queue reply fixture"))
                    .expect("stale queue.declare-ok");
                Method::read(stream)
                    ?
                    .check(0, 10, 50)?;
                Method::write(stream, 0, 10, 51, &[])
            },
            |broker| {
                with(
                    broker.addr(),
                    config::Config::new("guest", "guest").expect("config")
                        .capacity(config::Capacity::default().topology(
                            config::Topology::default().queues(1).identities(2),
                        )),
                    |rt| {
                        let conn = rt.connection();
                        assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                            let target = conn.observe().ready().await.expect("ready");
                            let channel = conn.channels().open_in(target, 1).await.expect("first channel");
                            let second = conn.channels().open_in(target, 2).await.expect("second channel");
                            let args = Table::new();
                            let options = queue::Declare {
                                queue: "", durable: false, exclusive: true,
                                auto_delete: false, arguments: &args,
                            };
                            let initial = channel.queue().declare(queue::Declare { ..options })
                                .await.expect("anonymous queue");
                            assert!(initial.queue.identity().is_some());
                            let name = initial.queue.as_ref().to_owned();
                            let held = retain.then_some(initial.queue);
                            let mut delete = Pinned::new(second.queue().delete(&name, false, false));
                            loop {
                                match events.try_recv() {
                                    Ok(()) => break,
                                    Err(mpsc::TryRecvError::Empty) => {
                                        assert!(poll_once(delete.as_pin()).await.is_pending());
                                        Yield::new().await;
                                    }
                                    result => { result.expect("broker ended before queue delete"); }
                                }
                            }
                            let (deleted, redecl) = Join::new(
                                delete,
                                channel.queue().declare(queue::Declare { queue: &name, ..options }),
                            ).await;
                            deleted.expect("delete original queue");
                            assert!(matches!(redecl, Err(ref error) if matches!(
                                error.error(), failure::Error::Protocol(Fault::TopologyQueue { .. })
                            )), "deleted redeclare must preserve its generation error");
                            if let Some(held) = held {
                                assert_eq!(held.as_ref(), name);
                            }
                        })).is_ok(), "AMQP fiber must complete");
                    }, recover,
                    |result| {
                        result.as_ref().map_err(|error| error.kind()).expect("endpoint completion");
                    },
                );
                broker.finish().expect("deleted redeclare broker")
                    .expect("deleted queue protocol close");
            },
        ).expect("deleted redeclare broker");
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
