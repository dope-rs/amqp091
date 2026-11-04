pub mod common;
use std::mem;

use common::plan;
use dope_amqp091::config::{Frame, Topology};
use dope_amqp091::failure::wire;
use dope_amqp091::service::Spec;
use dope_amqp091::{config, failure};

#[test]
fn frame_is_one_word() {
    std::assert_matches!(
        Frame::new(Frame::MIN),
        Ok(frame) if frame.get() == Frame::MIN
    );
    std::assert_matches!(
        Frame::new(Frame::MIN - 1),
        Err(failure::Error::InvalidConfig(wire::Config::FrameMinimum {
            actual,
            minimum: Frame::MIN,
        })) if actual == Frame::MIN - 1
    );
    assert_eq!(mem::size_of::<Frame>(), mem::size_of::<u32>());
}

#[test]
fn accepts_max_channels() {
    let config = config::Config::new("guest", "guest")
        .expect("valid credentials")
        .tune(config::Tune::default().channel_max(u16::MAX))
        .capacity(
            config::Capacity::default()
                .channels(usize::from(u16::MAX))
                .channel(config::Channel::new(1, 1, 1, 1, 1)),
        );
    Spec::<0>::try_new(config, 1, 1).expect("maximum channel plan");
}

#[test]
fn topology_retains_limits() {
    assert_eq!(Topology::default().get(), 16 * 1024 * 1024);
    assert_eq!(Topology::default().identity_cap(), 8192);
    let topology = Topology::new(7)
        .bytes(11)
        .identities(13)
        .exchanges(17)
        .queues(19)
        .routes(23)
        .links(29);
    assert_eq!(topology.get(), 11);
    assert_eq!(topology.identity_cap(), 13);
    assert_eq!(topology.exchange_cap(), 17);
    assert_eq!(topology.queue_cap(), 19);
    assert_eq!(topology.route_cap(), 23);
    assert_eq!(topology.link_cap(), 29);
    assert_eq!(mem::size_of::<Topology>(), 6 * mem::size_of::<usize>());
}

#[test]
fn rejects_cancel_overflow() {
    let each = (usize::try_from(u32::MAX / 2)).expect("u32 capacity must fit usize");
    let channel = config::Channel::default()
        .methods(0)
        .consumers(0)
        .confirms(each);
    let config = (config::Config::new("guest", "guest")).expect("valid credentials");
    let config = config
        .tune(config::Tune::default().channel_max(2))
        .capacity(config::Capacity::default().channels(2).channel(channel));
    assert!(matches!(
        Spec::<0>::try_new(config, 1, 1),
        Err(failure::Error::InvalidConfig(wire::Config::CancellationEntryCapacity {
            channels: 2, slots,
        })) if slots == u32::MAX / 2 + 3
    ));
}

#[test]
fn bounds_content_slots() {
    let queued = u32::MAX as usize;
    for (channel, expected) in [
        (
            config::Channel::default().deliveries(queued),
            wire::Route::Delivery,
        ),
        (
            config::Channel::default().returns(queued),
            wire::Route::Returned,
        ),
    ] {
        assert!(matches!(
            plan(config::Capacity::default().channels(1).channel(channel)),
            Err(failure::Error::InvalidConfig(wire::Config::ContentCount { route, error }))
                if route == expected && error.requested() == queued + 1
        ));
    }
}

#[test]
fn requires_terminal_causes() {
    assert!(matches!(
        plan(config::Capacity::default().causes(0)),
        Err(failure::Error::InvalidConfig(
            wire::Config::TerminalCausesZero
        ))
    ));
}

#[test]
fn bounds_terminal_causes() {
    assert!(matches!(
        plan(config::Capacity::default().causes(usize::MAX)),
        Err(failure::Error::InvalidConfig(
            wire::Config::TerminalCauses {
                actual: usize::MAX,
                ..
            }
        ))
    ));
}

#[test]
fn bounds_identity_slots() {
    assert!(matches!(
        plan(config::Capacity::default().topology(Topology::default().identities(usize::MAX))),
        Err(failure::Error::InvalidConfig(wire::Config::TopologyIdentities { error }))
            if error.requested() == usize::MAX
    ));
}

#[test]
fn bounds_topology_slots() {
    for (topology, expected) in [
        (
            Topology::default().exchanges(usize::MAX),
            wire::Limit::Exchange,
        ),
        (Topology::default().queues(usize::MAX), wire::Limit::Queue),
        (
            Topology::default().routes(usize::MAX),
            wire::Limit::QueueBind,
        ),
        (
            Topology::default().links(usize::MAX),
            wire::Limit::ExchangeBind,
        ),
    ] {
        assert!(matches!(
            plan(config::Capacity::default().topology(topology)),
            Err(failure::Error::InvalidConfig(wire::Config::TopologyCount { target, error }))
                if target == expected && error.requested() == usize::MAX
        ));
    }
}

#[test]
fn rejects_zero_capacity() {
    assert!(matches!(
        plan(config::Capacity::default().channels(0)),
        Err(failure::Error::InvalidConfig(
            wire::Config::CancellationChannelZero
        ))
    ));
}

#[test]
fn rejects_egress_bytes() {
    assert!(matches!(
        plan(config::Capacity::default().egress(config::Egress::new(2, u32::MAX as usize))),
        Err(failure::Error::InvalidConfig(
            wire::Config::OutboundResident {
                frames: 2,
                bytes: u32::MAX
            }
        ))
    ));
}

#[test]
fn rejects_empty_egress() {
    assert!(matches!(
        plan(config::Capacity::default().egress(config::Egress::new(0, 4096))),
        Err(failure::Error::InvalidConfig(wire::Config::OutboundZero))
    ));
}
