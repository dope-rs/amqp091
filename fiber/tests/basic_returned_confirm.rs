use crate::common::amqp::Peer;
use crate::common::amqp::app::Endpoint;
use crate::common::amqp::wire::Method;
use crate::common::recovery::client::complete;
use crate::common::recovery::recover;
pub mod common;

use dope::manifold::timing;
use dope_amqp091::message::Builder;
use dope_amqp091::protocol::Confirmation;
use dope_amqp091::{adapter, config};
use dope_test::harness::peer::TcpScript;

use crate::common::basic;
use crate::common::basic::returned;

#[test]
fn precedes_confirm() {
    TcpScript::with(
        |stream| {
            Peer::new(stream)
                .open(basic::TUNE)
                .expect("AMQP broker handshake");
            let channel = returned::open_channel(stream).expect("AMQP fixture");
            Method::read(&mut *stream)
                .expect("confirm.select")
                .check(channel, 85, 10)
                .expect("AMQP method route");
            Method::write(stream, channel, 85, 11, &[]).expect("confirm.select-ok");

            returned::check_publish(
                stream,
                channel,
                basic::EXCHANGE,
                basic::ROUTING_KEY,
                basic::BODY,
                Some(returned::CONTENT_TYPE),
            )
            .expect("AMQP fixture");
            stream
                .write_all(
                    &returned::content(
                        channel,
                        basic::EXCHANGE,
                        basic::ROUTING_KEY,
                        &[b"fragmented-", b"body"],
                        Some(returned::CONTENT_TYPE),
                    )
                    .expect("AMQP fixture"),
                )
                .expect("basic.return content");
            let mut ack = 1_u64.to_be_bytes().to_vec();
            ack.push(0);
            Method::write(stream, channel, 60, 80, &ack).expect("basic.ack after return");
            returned::close_connection(stream).expect("AMQP fixture");
        },
        |broker| {
            let channel = config::Channel::default().methods(1).confirms(1).returns(1);
            let capacity = config::Capacity::default()
                .channels(1)
                .channel(channel)
                .egress(config::Egress::new(4, 4096));
            let config = config::Config::new("guest", "guest")
                .expect("AMQP config")
                .capacity(capacity);

            Endpoint::new(broker.addr(), config).try_check::<timing::Throughput, _, _>(
                basic::POLICY,
                |rt| {
                    let conn = rt.connection();
                    assert!(rt.block_on(dope_fiber::fiber!('_ => async move {
                let ready = conn.observe().ready().await.expect("ready");
                let channel = conn.channels().open(1).await.expect("channel open");
                let confirms = channel.confirm().select().await.expect("confirm.select");
                let properties = Builder::default()
                    .content_type("obsolete")
                    .expect("initial content type")
                    .content_type(returned::CONTENT_TYPE)
                    .expect("content type")
                    .build();
                let outcome = confirms
                    .publish(adapter::publish::Request {
                        exchange: basic::EXCHANGE,
                        routing_key: basic::ROUTING_KEY,
                        mandatory: true,
                        properties: &properties,
                        body: basic::BODY,
                    })
                    .await
                    .expect("mandatory publish confirm");
                assert_eq!(outcome, Confirmation::Ack);

                let returned = channel
                    .next_return()
                    .await
                    .expect("return state")
                    .expect("basic.return");
                assert_eq!(returned.reply_code(), returned::REPLY_CODE);
                assert_eq!(returned.reply_text(), returned::REPLY_TEXT);
                assert_eq!(returned.exchange(), basic::EXCHANGE);
                assert_eq!(returned.routing_key(), basic::ROUTING_KEY);
                assert_eq!(
                    returned.properties().content_type(),
                    Some(returned::CONTENT_TYPE)
                );
                assert!(returned.body() == basic::BODY[..]);
                returned.release();
                conn
                    .control()
                    .close(ready, 200, "official return complete")
                    .await
                    .expect("connection close");
            })).is_ok(), "AMQP fiber must complete");
                },
                recover,
                complete,
            );
            broker.finish().expect("scripted AMQP broker completion");
        },
    )
    .expect("scripted AMQP broker");
}
