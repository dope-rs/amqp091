use dope_amqp091::content::properties::{BasicProperties, ContentHeader};
use o3::buffer::Block;

#[test]
fn compact_properties_round_trip() {
    assert!(std::mem::size_of::<BasicProperties>() <= 128);

    let properties = BasicProperties::builder()
        .content_type("application/json")
        .content_encoding("utf-8")
        .correlation_id("correlation")
        .reply_to("reply")
        .expiration("30000")
        .message_id("message")
        .kind("event")
        .user_id("user")
        .app_id("app")
        .reserved("reserved")
        .delivery_mode(2)
        .priority(7)
        .timestamp(42)
        .build();
    let mut payload = Block::new();
    properties.encode_header_frame(&mut payload, 123);

    let header = ContentHeader::read(payload.freeze()).unwrap();
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
