use futures::stream::{Stream, StreamExt};
use std::borrow::Cow;
use std::pin::Pin;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tonic::{Request, Response, Status};
use tonic_health::pb::{
    health_check_response::ServingStatus, health_server::Health, HealthCheckRequest,
    HealthCheckResponse,
};

const HEALTH_CHECK_ALL_PSEUDO_SERVICE: &str = "discord.v1.HealthCheckAll";

pub trait HealthChecker: Send + Sync + 'static {
    fn is_healthy(&self) -> bool;
}

impl<F: Fn() -> bool + Send + Sync + 'static> HealthChecker for F {
    fn is_healthy(&self) -> bool {
        self()
    }
}

/// For any type `T` implementing `HealthChecker`,
/// `Arc<T>` also implements `HealthChecker`.
/// This permits passing `Arc<T>` anywhere that accepts an `impl HealthChecker`, e.g. `fn foo(x: impl HealthChecker)`
impl<T: HealthChecker> HealthChecker for Arc<T> {
    fn is_healthy(&self) -> bool {
        self.as_ref().is_healthy()
    }
}

/// A server readiness check that always returns healthy.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct AlwaysHealthy {}

impl HealthChecker for AlwaysHealthy {
    fn is_healthy(&self) -> bool {
        true
    }
}

#[derive(Clone)]
pub(crate) struct HealthServer {
    callbacks: Arc<HashMap<Cow<'static, str>, Vec<Box<dyn HealthChecker>>>>,
    shutting_down: tokio::sync::watch::Receiver<bool>,
}

impl HealthServer {
    pub(crate) fn new(
        callbacks: impl Into<Arc<HashMap<Cow<'static, str>, Vec<Box<dyn HealthChecker>>>>>,
    ) -> (Self, ShutdownSender) {
        let (tx, rx) = tokio::sync::watch::channel(false);
        (
            HealthServer {
                callbacks: callbacks.into(),
                shutting_down: rx,
            },
            ShutdownSender { inner: tx },
        )
    }

    fn is_healthy(
        checks: &[Box<dyn HealthChecker>],
        shutting_down: &tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        !*shutting_down.borrow() && checks.iter().all(|c| c.is_healthy())
    }

    fn is_healthy_and_update(
        checks: &[Box<dyn HealthChecker>],
        shutting_down: &mut tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        !*shutting_down.borrow_and_update() && checks.iter().all(|c| c.is_healthy())
    }

    fn is_all_services_healthy(&self) -> bool {
        if *self.shutting_down.borrow() {
            return false;
        }

        for checks in self.callbacks.values() {
            for check in checks {
                if !check.is_healthy() {
                    return false;
                }
            }
        }

        true
    }
}

#[tonic::async_trait]
impl Health for HealthServer {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let service_name = request.get_ref().service.as_str();
        if service_name.is_empty() {
            // Liveness check should always return Serving.
            return Ok(Response::new(bool_to_health_response(true)));
        }

        if service_name == HEALTH_CHECK_ALL_PSEUDO_SERVICE {
            return Ok(Response::new(bool_to_health_response(
                self.is_all_services_healthy(),
            )));
        }

        match self.callbacks.get(service_name) {
            Some(checks) => Ok(Response::new(bool_to_health_response(
                HealthServer::is_healthy(checks, &self.shutting_down),
            ))),
            None => Err(not_found_error(service_name)),
        }
    }

    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<HealthCheckResponse, Status>> + Send + 'static>>;

    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let service_name = request.get_ref().service.to_string();
        if service_name.is_empty() {
            // Liveness check should always return Serving,
            // and then stay open forever.
            return Ok(Response::new(Box::pin(
                futures::stream::once(async { Ok(bool_to_health_response(true)) })
                    .chain(futures::stream::pending()),
            )));
        }
        let HealthServer {
            callbacks,
            mut shutting_down,
        } = self.clone();
        let initial_value = match callbacks.get(service_name.as_str()) {
            Some(checks) => HealthServer::is_healthy_and_update(checks, &mut shutting_down),
            None => {
                return Err(not_found_error(&service_name));
            }
        };

        let output = async_stream::try_stream! {
            yield bool_to_health_response(initial_value);

            let checks = &callbacks[service_name.as_str()];
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            let mut prev_value = initial_value;
            loop {
                tokio::select!{
                    _ = interval.tick() => {},
                    _ = shutting_down.changed() => {},
                };
                let new_value = HealthServer::is_healthy_and_update(checks, &mut shutting_down);
                if new_value != prev_value {
                    yield bool_to_health_response(new_value);
                    prev_value = new_value;
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::WatchStream))
    }
}

pub(crate) struct ShutdownSender {
    inner: tokio::sync::watch::Sender<bool>,
}

impl ShutdownSender {
    pub(crate) fn notify(&self) {
        // TODO: Replace with send_if_modified once Tokio is upgraded.
        if !*self.inner.borrow() {
            let _ = self.inner.send(true);
        }
    }
}

fn bool_to_health_response(healthy: bool) -> HealthCheckResponse {
    HealthCheckResponse {
        status: if healthy {
            ServingStatus::Serving as i32
        } else {
            ServingStatus::NotServing as i32
        },
    }
}

fn not_found_error(service_name: &str) -> Status {
    Status::not_found(format!("service {} not registered", service_name))
}

// TODO copy over tests
