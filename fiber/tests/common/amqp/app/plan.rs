use dope_amqp091::{config, failure};

pub fn confirms(confirms: usize) -> Result<config::Config, failure::Error> {
    let chan = config::Channel::default().confirms(confirms);
    let cap = config::Capacity::default().channel(chan);
    Ok(config::Config::new("guest", "guest")?.capacity(cap))
}

pub fn returns() -> Result<config::Config, failure::Error> {
    let chan = config::Channel::default().methods(1).returns(1);
    let cap = config::Capacity::default()
        .channels(1)
        .channel(chan)
        .egress(config::Egress::new(4, 4096));
    Ok(config::Config::new("guest", "guest")?.capacity(cap))
}
