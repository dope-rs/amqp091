use std::convert::Infallible;

use dope::core::driver::life::creation::Attempt;
use dope::core::driver::life::{domain, recovery};
use dope::core::driver::settings;
use dope::core::io::Reclaimed;
use dope::runtime::executor::{Executor, create};
use dope_amqp091::service::{Build, Port, Spec};
use dope_amqp091::{config, failure};
use dope_test::alloc::{Activity, Tracker};

pub struct Check(pub Option<usize>);

#[derive(Debug)]
pub enum Error<'a> {
    Executor(create::Kind<'a>),
    Domain(domain::Cause<&'a Infallible>),
    Config(failure::Error),
    Reports(Vec<Reclaimed>),
}

struct Profile;

impl settings::Profile for Profile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
    const SCHEDULER: settings::Schedule = settings::Schedule::fixed::<64, 0>();
    const FILES: settings::FileSlots = settings::FileSlots::fixed::<0, 1>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<2, 4096>();
    const RESIDENT_DOMAINS: settings::Domain = settings::Domain::fixed::<2>();
}

fn config() -> Result<config::Config, failure::Error> {
    Ok(config::Config::new("guest", "guest")?
        .tune(config::Tune::default().channel_max(2))
        .capacity(config::Capacity::new(
            2,
            config::Channel::new(1, 1, 1, 1, 1),
            config::Body::new(4096, 4096, 2),
            config::Egress::new(1, 4096),
        )))
}

impl Check {
    pub fn build<R>(
        self,
        recover: impl recovery::Recover,
        finish: impl for<'a> FnOnce(Result<((Result<usize, Build>, usize), Activity), Error<'a>>) -> R,
    ) -> R {
        let settings = settings::Config::for_profile::<Profile>();
        let mut creation = Attempt::new();
        let executor = match Executor::new(&mut creation, settings) {
            Ok(executor) => executor,
            Err(cause) => return finish(Err(Error::Executor(cause.kind()))),
        };
        let mut reports = Vec::new();
        executor.run(|active| {
            let output = active.enter_with(
                |mut session| {
                    Tracker::activity(|| {
                        let config = config()?;
                        let spec = Spec::<0>::try_new(config, 1, 1)?;
                        let run = || {
                            Port::try_with(
                                spec,
                                &mut session,
                                |session| session.driver(),
                                |port, _| port.capacity().count().get(),
                            )
                        };
                        Ok::<_, failure::Error>(Tracker::count(|| match self.0 {
                            Some(index) => Tracker::fail_after(index, run),
                            None => run(),
                        }))
                    })
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            if !reports.is_empty() {
                return finish(Err(Error::Reports(reports)));
            }
            let (output, activity) = match output {
                Ok(output) => output,
                Err(cause) => return finish(Err(Error::Domain(cause.cause()))),
            };
            finish(
                output
                    .map(|output| (output, activity))
                    .map_err(Error::Config),
            )
        })
    }
}
