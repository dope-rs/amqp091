use dope_amqp091::config::Config;
use dope_amqp091::failure;
use dope_amqp091::failure::wire;
use dope_amqp091::protocol::wire::text::short::{Resource, Short};

#[test]
fn enforces_name_limit() {
    let accepted = "x".repeat(127);
    let rejected = "x".repeat(128);

    assert!(Short::resource(&accepted, Resource::Queue).is_ok());
    for (resource, kind) in [
        (Resource::Queue, wire::Encoding::Queue),
        (Resource::Exchange, wire::Encoding::Exchange),
        (Resource::Short, wire::Encoding::Short),
    ] {
        assert!(matches!(
            Short::resource(&rejected, resource),
            Err(failure::Error::Encoding(wire::Encode::Length {
                kind: found,
                actual: 128,
                max: 127,
            })) if found == kind
        ));
    }
    let config = (Config::new("guest", "guest")).expect("valid credentials");
    assert!(config.vhost(&accepted).is_ok());
    let config = (Config::new("guest", "guest")).expect("valid credentials");
    assert!(matches!(
        config.vhost(&rejected),
        Err(failure::Error::Encoding(wire::Encode::Length {
            kind: wire::Encoding::VirtualHost,
            actual: 128,
            max: 127,
        }))
    ));
}
