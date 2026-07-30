use dope_amqp091::content::properties::{BasicProperties, ContentHeader};
use dope_amqp091::wire::sink::BoundedSink;
use o3::buffer::BLOCK_CAPACITY;

#[test]
fn compact_properties_round_trip() {
    assert!(std::mem::size_of::<BasicProperties>() <= 128);

    let properties = BasicProperties::builder()
        .content_type("application/json")
        .unwrap()
        .content_encoding("utf-8")
        .unwrap()
        .correlation_id("correlation")
        .unwrap()
        .reply_to("reply")
        .unwrap()
        .expiration("30000")
        .unwrap()
        .message_id("message")
        .unwrap()
        .kind("event")
        .unwrap()
        .user_id("user")
        .unwrap()
        .app_id("app")
        .unwrap()
        .reserved("reserved")
        .unwrap()
        .delivery_mode(2)
        .priority(7)
        .timestamp(42)
        .build();
    let mut payload = BoundedSink::<BLOCK_CAPACITY>::new();
    properties.encode_header_frame(&mut payload, 123);

    let header = ContentHeader::read(payload.finish().unwrap().freeze()).unwrap();
    assert_eq!(header.body_size, 123);
    assert_eq!(header.properties.content_type(), Some("application/json"));
    assert_eq!(header.properties.content_encoding(), Some("utf-8"));
    assert_eq!(header.properties.correlation_id(), Some("correlation"));
    assert_eq!(header.properties.reply_to(), Some("reply"));
    assert_eq!(header.properties.expiration(), Some("30000"));
    assert_eq!(header.properties.message_id(), Some("message"));
    assert_eq!(header.properties.kind(), Some("event"));
    assert_eq!(header.properties.user_id(), Some("user"));
    assert_eq!(header.properties.app_id(), Some("app"));
    assert_eq!(header.properties.reserved(), Some("reserved"));
    assert_eq!(header.properties.delivery_mode, Some(2));
    assert_eq!(header.properties.priority, Some(7));
    assert_eq!(header.properties.timestamp, Some(42));
}
