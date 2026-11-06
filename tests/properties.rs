use dope_amqp091::message::Builder;

#[test]
fn round_trips_properties() {
    let value = "x".repeat(255);
    let mut builder = Builder::default();
    for _ in 0..257 {
        builder = builder
            .content_type(&value)
            .expect("replace full-length content type");
    }
    let props = builder
        .content_type("obsolete")
        .expect("initial content type")
        .encoding("latin")
        .expect("valid content encoding")
        .correlation("αβγ")
        .expect("valid correlation id")
        .reply_to("reply")
        .expect("valid reply target")
        .expiration("30000")
        .expect("valid expiration")
        .message_id("message")
        .expect("valid message id")
        .kind("event")
        .expect("valid message kind")
        .user_id("user")
        .expect("valid user id")
        .app_id("app")
        .expect("valid application id")
        .reserved("")
        .expect("empty reserved value")
        .delivery(2)
        .expect("persistent delivery mode")
        .priority(7)
        .expect("valid priority")
        .timestamp(42)
        .encoding("utf-8")
        .expect("equal-length replacement")
        .correlation("αβ")
        .expect("shorter UTF-8 replacement")
        .content_type("application/json")
        .expect("longer replacement")
        .build();
    assert_eq!(props.content_type(), Some("application/json"));
    assert_eq!(props.encoding(), Some("utf-8"));
    assert_eq!(props.correlation(), Some("αβ"));
    assert_eq!(props.reply_to(), Some("reply"));
    assert_eq!(props.expiration(), Some("30000"));
    assert_eq!(props.message_id(), Some("message"));
    assert_eq!(props.kind(), Some("event"));
    assert_eq!(props.user_id(), Some("user"));
    assert_eq!(props.app_id(), Some("app"));
    assert_eq!(props.reserved(), Some(""));
    assert_eq!(props.delivery(), Some(2));
    assert_eq!(props.priority(), Some(7));
    assert_eq!(props.timestamp, Some(42));
}
