use dope::runtime::executor::Executor;
use dope::runtime::profile::Throughput;
use dope_amqp091::config::Config;
use dope_amqp091::protocol::session::port::AmqpPort;

#[test]
fn port_builds_when_the_driver_has_no_fast_path_timer_slots() {
    let mut driver_config = dope::driver::Config::for_profile::<Throughput>();
    driver_config.timer_slots = 0;
    Executor::new(driver_config)
        .expect("executor")
        .enter(|mut session| {
            let config = Config::new("guest", "guest")
                .buffer_capacity(4096)
                .body_fragment_capacity(4)
                .outbound_capacity(4)
                .outbound_buffer_capacity(4096);
            AmqpPort::new(config, 1, 4, &mut session.driver_access()).expect("AMQP port");
        });
}
