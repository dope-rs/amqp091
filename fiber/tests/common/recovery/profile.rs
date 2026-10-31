use dope::core::driver::settings;
use dope::manifold::timing::Policy;
use o3::base::time::Positive;

pub struct Profile;

impl settings::Profile for Profile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<4096, 65_536>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<1024, { 64 * 1024 }>();
}

impl Policy for Profile {
    const CONNECT_DEADLINE: Positive = Positive::from_secs::<2>();
    const IDLE_WINDOW: Positive = Positive::from_secs::<5>();
    const SEND_DEADLINE: Positive = Positive::from_secs::<2>();
}

pub struct Startup;

impl settings::Profile for Startup {
    const QUEUES: settings::QueueLayout = <Profile as settings::Profile>::QUEUES;
    const RECEIVE: settings::Receive = <Profile as settings::Profile>::RECEIVE;
}

impl Policy for Startup {
    const CONNECT_DEADLINE: Positive = Profile::CONNECT_DEADLINE;
    const IDLE_WINDOW: Positive = Positive::from_millis::<500>();
    const SEND_DEADLINE: Positive = Profile::SEND_DEADLINE;
}
