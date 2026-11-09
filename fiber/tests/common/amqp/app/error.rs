use std::convert::Infallible;

use dope::core::driver::life::domain;
use dope::core::driver::settings::errors;
use dope::core::io;
use dope::manifold::app::creation;
use dope::runtime::executor::{create, session};
use dope_amqp091::service::app;
use dope_amqp091::{failure, service};

type Output<'owner, R> =
    Result<Result<Result<R, session::Error<'owner, R>>, creation::Error>, service::Build>;

pub enum Error<'owner, R, Q = ()> {
    Reports(Vec<io::Reclaimed>),
    Config(failure::Error),
    Plan(app::Error),
    Claim(errors::Claim),
    Executor(create::Error<'owner>),
    Domain(domain::Error<'owner, Infallible, Output<'owner, R>, Q>),
    Storage(service::Build),
    Install(creation::Error),
    App(session::Error<'owner, R>),
}

#[derive(Debug)]
pub enum Kind<'a, 'owner, R> {
    Reports(&'a [io::Reclaimed]),
    Config(&'a failure::Error),
    Plan(&'a app::Error),
    Claim(&'a errors::Claim),
    Executor(create::Kind<'a>),
    Close {
        result: &'a Result<Output<'owner, R>, Infallible>,
        failure: domain::Unmap,
    },
    Storage(&'a service::Build),
    Install(&'a creation::Error),
    App(&'a session::Error<'owner, R>),
}

impl<'owner, R, Q> Error<'owner, R, Q> {
    pub fn kind(&self) -> Kind<'_, 'owner, R> {
        match self {
            Self::Reports(reports) => Kind::Reports(reports),
            Self::Config(error) => Kind::Config(error),
            Self::Plan(error) => Kind::Plan(error),
            Self::Claim(error) => Kind::Claim(error),
            Self::Executor(error) => Kind::Executor(error.kind()),
            Self::Domain(error) => match error {
                domain::Error::Storage(error) => match *error {},
                domain::Error::Close(state) => Kind::Close {
                    result: state.result(),
                    failure: state.failure(),
                },
            },
            Self::Storage(error) => Kind::Storage(error),
            Self::Install(error) => Kind::Install(error),
            Self::App(error) => Kind::App(error),
        }
    }
}
