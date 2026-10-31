use dope::core::driver::settings;
use dope::manifold::timing::Throughput;
use dope_amqp091::{config, failure};

pub fn config() -> Result<config::Config, failure::Error> {
    Ok(config::Config::new("guest", "guest")?.capacity(
        config::Capacity::default()
            .body(config::Body::default().buffer(4096).fragments(4))
            .egress(config::Egress::new(4, 4096)),
    ))
}

pub fn profile() -> settings::Config {
    settings::Config::for_profile::<Throughput>().with_domains(settings::Domain::fixed::<2>())
}
