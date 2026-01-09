use std::{
    any::Any,
    future::Future,
    time::{Duration, Instant},
};

use crate::gcloud::pubsub::PubSubSubscription;
use crate::tokio_utils::AbortOnDrop;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

use crate::pub_sub_streaming_pull::StreamingPullManagerMessage;

/// Returned by [`crate::StreamingPullManager::new`]. Can be used to stop the manager.
///
/// If this manager handle is dropped, the streaming pull manager is immediately hard stopped.
#[must_use]
pub struct StreamingPullManagerHandle {
    sender: UnboundedSender<StreamingPullManagerMessage>,
    client_id: uuid::Uuid,
    subscription: PubSubSubscription,
    state: HandleState,
}

#[derive(Debug)]
enum HandleState {
    Running(AbortOnDrop<Instant>),
    GracefullyStopped(Instant),
    PreviouslyPanicked,
}

impl HandleState {
    fn take_abort_on_drop(&mut self) -> Option<AbortOnDrop<Instant>> {
        let taken = std::mem::replace(self, Self::PreviouslyPanicked);
        match taken {
            Self::Running(abort_on_drop) => Some(abort_on_drop),
            _ => None,
        }
    }

    async fn join(&mut self) -> Result<Instant, StreamingPullManagerPanicError> {
        match self {
            Self::Running(abort_on_drop) => {
                let result = abort_on_drop.await;
                match result {
                    Ok(instant) => {
                        *self = Self::GracefullyStopped(instant);
                        Ok(instant)
                    }
                    Err(join_error) => {
                        *self = Self::PreviouslyPanicked;
                        Err(StreamingPullManagerPanicError::Panicked {
                            panic: join_error.into_panic(),
                        })
                    }
                }
            }
            Self::GracefullyStopped(instant) => Ok(*instant),
            Self::PreviouslyPanicked => Err(StreamingPullManagerPanicError::PreviouslyPanicked),
        }
    }
}

impl StreamingPullManagerHandle {
    pub(crate) fn new(
        subscription: PubSubSubscription,
        client_id: uuid::Uuid,
        sender: UnboundedSender<StreamingPullManagerMessage>,
        manager_abort_on_drop: AbortOnDrop<Instant>,
    ) -> Self {
        Self {
            sender,
            client_id,
            subscription,
            state: HandleState::Running(manager_abort_on_drop),
        }
    }

    pub fn client_id(&self) -> uuid::Uuid {
        self.client_id
    }

    pub fn subscription(&self) -> &PubSubSubscription {
        &self.subscription
    }

    /// Attempts to gracefully stop the manager. The graceful stop might fail because the manager had panicked already.
    ///
    /// Returns a [`StreamingPullManagerStoppingHandle`] which can be used to hard stop the manager after a specific
    /// amount of time has passed.
    pub async fn graceful_stop(
        mut self,
    ) -> Result<StreamingPullManagerStoppingHandle, StreamingPullManagerPanicError> {
        let abort_on_drop = match self.state.take_abort_on_drop() {
            Some(abort_on_drop) => abort_on_drop,
            None => return Err(StreamingPullManagerPanicError::PreviouslyPanicked),
        };

        let (sender, receiver) = oneshot::channel();
        let graceful_stop_initiated_at = Instant::now();

        tracing::info!(
            {subscription = %self.subscription, client_id = %self.client_id},
            "streaming pull manager handler requested graceful stop"
        );

        self.sender
            .send(StreamingPullManagerMessage::GracefulStop { sender })
            .ok();

        match receiver.await {
            // The streaming pull manager has acknowledged our request to gracefully stop.
            Ok(Ok(())) => Ok(StreamingPullManagerStoppingHandle {
                graceful_stop_initiated_at,
                state: HandleState::Running(abort_on_drop),
                client_id: self.client_id,
                subscription: self.subscription
            }),
            // This should never happen, but we can't prove this to rust.
            Ok(Err(_)) => unreachable!(
                "invariant: there can not be another graceful stop requested,
                 because graceful_stop can only be called once (because the graceful stop method is consuming)"
            ),
            // The streaming pull manager had dropped our oneshot, that means that it must have stopped, figure out why:
            Err(_) => match abort_on_drop.await {
                // The streaming pull manager had already cleanly stopped, but at who's behest?
                Ok(_) => unreachable!(
                    "invariant: the streaming pull manager had cleanly stopped for an unknown reason?"
                ),
                // The streaming pull manager must have panicked :(.
                Err(join_error) => Err(StreamingPullManagerPanicError::Panicked {
                    panic: join_error.into_panic(),
                }),
            },
        }
    }

    /// Monitors the manager, resolving if the manager unexpectedly stops.
    ///
    /// This is useful to detect if the manager has panicked, and to cascade the failure to the rest of the system.
    ///
    /// # Cancel Safety:
    ///
    /// This function is cancel safe, and will not lose any error states if the future is cancelled prior to completion.
    pub async fn monitor(&mut self) -> StreamingPullManagerPanicError {
        match self.state.join().await {
            // The streaming pull manager stopped, for unknown reason. This is a bug.
            Ok(_) => unreachable!(
                "invariant: the streaming pull manager had cleanly stopped, without us calling `graceful_stop`",
            ),
            // The streaming pull manager must have panicked.
            Err(error) => error,
        }
    }

    /// This is a convenience method which will provide a ctrl-c handler to stop a streaming pull manager.
    ///
    /// This method writes output to stderr instead of using the tracing logging facilities, as this ctrl-c handling is
    /// generally done when developing a service. This method should not be used in production, as it does not handle
    /// SIGTERM, which is usually what kubernetes and other job managers will use to signal the job to stop.
    ///
    /// The stop procedure is as follows:
    ///    1. Wait for ctrl-c, or the manager to unexpectedly panic. If the manager had panicked, propagate that panic.
    ///    2. Initiate a graceful stop of the manager.
    ///    3. Wait till either `graceful_stop_timeout` has elapsed, or ctrl-c is hit again.
    ///    4. Hard stop the manager.
    pub async fn gracefully_stop_on_ctrl_c(
        mut self,
        graceful_stop_timeout: Duration,
    ) -> StreamingPullManagerStopped {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            error = self.monitor() => error.continue_panicking()
        }

        eprintln!(
            "[streaming pull manager] ctrl-c received, gracefully stopping streaming pull manager (subscription={}, client_id={})",
            self.subscription, self.client_id
        );

        let mut stopping = match self.graceful_stop().await {
            Ok(stopping) => stopping,
            Err(error) => error.continue_panicking(),
        };

        eprintln!(
            "[streaming pull manager] graceful stop has begun. will wait {:?} until hard stopping. press ctrl-c again to hard stop immediately.",
            graceful_stop_timeout,
        );

        enum SelectResult {
            StopResult(
                Result<StreamingPullManagerGracefullyStopped, StreamingPullManagerPanicError>,
            ),
            StopNow {
                ctrl_c: bool,
            },
        }

        let select_result = tokio::select! {
            stop_result = stopping.join() => SelectResult::StopResult(stop_result),
            _ = tokio::time::sleep(graceful_stop_timeout) => SelectResult::StopNow { ctrl_c: false },
            _ = tokio::signal::ctrl_c() => SelectResult::StopNow { ctrl_c: true },
        };

        match select_result {
            SelectResult::StopResult(Ok(gracefully)) => {
                eprintln!(
                    "[streaming pull manager] graceful stop has succeeded, took {:?}",
                    gracefully.duration
                );
                gracefully.into()
            }
            SelectResult::StopResult(Err(panicked)) => panicked.continue_panicking(),
            SelectResult::StopNow { ctrl_c } => {
                if ctrl_c {
                    eprintln!("[streaming pull manager] ctrl-c hit again. hard stopping now.");
                } else {
                    eprintln!(
                        "[streaming pull manager] {:?} has elapsed, hard stopping now.",
                        graceful_stop_timeout
                    );
                }

                match stopping.hard_stop_now().await {
                    Ok(StreamingPullManagerStopped::Gracefully(gracefully)) => {
                        eprintln!(
                            "[streaming pull manager] even though a hard stop was requested, the streaming pull manager beat us to the punch and gracefully stopped, took: {:?}",
                            gracefully.duration
                        );
                        gracefully.into()
                    }
                    Ok(StreamingPullManagerStopped::Hard(hard)) => {
                        eprintln!(
                            "[streaming pull manager] hard stop completed, took {:?}",
                            hard.duration
                        );
                        hard.into()
                    }
                    Err(panicked) => panicked.continue_panicking(),
                }
            }
        }
    }

    /// This is a convenience method which will gracefully stop the streaming pull manager when the provided future
    /// resolves, waiting up to `graceful_stop_timeout` before hard stopping the manager.
    ///
    /// This method will panic if the streaming pull manager panics.
    pub async fn gracefully_stop_on_signal(
        mut self,
        signal: impl Future<Output = ()>,
        graceful_stop_timeout: Duration,
    ) -> StreamingPullManagerStopped {
        tokio::select! {
            _ = signal => {},
            error = self.monitor() => error.continue_panicking()
        }

        let subscription = self.subscription.clone();
        let client_id = self.client_id;

        tracing::warn!(
            {%subscription, %client_id},
            "signal received, gracefully stopping streaming pull manager",
        );

        let stopping = match self.graceful_stop().await {
            Ok(stopping) => stopping,
            Err(error) => error.continue_panicking(),
        };

        tracing::warn!(
            {%subscription, %client_id},
            "graceful stop has begun. will wait {:?} until hard stopping.",
            graceful_stop_timeout,
        );

        match stopping.hard_stop_after(graceful_stop_timeout).await {
            Err(error) => error.continue_panicking(),
            Ok(StreamingPullManagerStopped::Gracefully(gracefully)) => {
                tracing::warn!(
                    {%subscription, %client_id},
                    "gracefullly stopped in {:?}",
                    gracefully.duration,
                );
                gracefully.into()
            }
            Ok(StreamingPullManagerStopped::Hard(hard)) => {
                tracing::warn!(
                    {%subscription, %client_id},
                    "hard stopped in {:?} (hard stop took {:?} to process)",
                    hard.duration, hard.abort_duration
                );
                hard.into()
            }
        }
    }
}

#[derive(Debug)]
pub enum StreamingPullManagerPanicError {
    /// Streaming pull manager has panicked.
    Panicked {
        /// The panic that was thrown from the streaming pull manager.
        panic: Box<dyn Any + Send + 'static>,
    },
    /// The streaming pull manager had panicked in the past, and [`StreamingPullManagerPanicError::Panicked`] was
    /// returned.
    PreviouslyPanicked,
}

impl StreamingPullManagerPanicError {
    /// Continues the panic that started in the manager.
    pub fn continue_panicking(self) -> ! {
        match self {
            StreamingPullManagerPanicError::Panicked { panic } => std::panic::resume_unwind(panic),
            StreamingPullManagerPanicError::PreviouslyPanicked => {
                panic!("manager had previously panicked")
            }
        }
    }
}

impl std::fmt::Display for StreamingPullManagerPanicError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamingPullManagerPanicError::PreviouslyPanicked => {
                write!(fmt, "previously panicked")
            }
            StreamingPullManagerPanicError::Panicked { .. } => {
                write!(fmt, "panicked")
            }
        }
    }
}

impl std::error::Error for StreamingPullManagerPanicError {}

#[derive(Debug)]
#[must_use]
pub struct StreamingPullManagerStoppingHandle {
    client_id: uuid::Uuid,
    subscription: PubSubSubscription,
    graceful_stop_initiated_at: Instant,
    state: HandleState,
}

#[derive(Debug)]
pub struct StreamingPullManagerGracefullyStopped {
    /// How long it took for the graceful stop to complete, measured as the time spent since
    /// [`StreamingPullManagerHandle::graceful_stop`] was called.
    pub duration: Duration,
    /// The instant in which the graceful stop had completed.
    pub instant: Instant,
}

impl StreamingPullManagerGracefullyStopped {
    fn new(graceful_stop_started_at: Instant, graceful_stop_completed_at: Instant) -> Self {
        Self {
            duration: graceful_stop_completed_at
                .saturating_duration_since(graceful_stop_started_at),
            instant: graceful_stop_completed_at,
        }
    }
}

#[derive(Debug)]
pub struct StreamingPullManagerHardStopped {
    /// How long it took for the stop to complete, measured as the time spent since
    /// [`StreamingPullManagerHandle::graceful_stop`] was called.
    pub duration: Duration,
    /// How long between when we tried to `abort` the StreamingPullManager, and when it fully stopped.
    pub abort_duration: Duration,
    /// The instant in which the hard stop had completed.
    pub instant: Instant,
}

#[derive(Debug)]
pub enum StreamingPullManagerStopped {
    /// The streaming pull manager has successfully gracefully stopped.
    Gracefully(StreamingPullManagerGracefullyStopped),
    /// The streaming pull manager has successfully hard stopped.
    Hard(StreamingPullManagerHardStopped),
}

impl From<StreamingPullManagerGracefullyStopped> for StreamingPullManagerStopped {
    fn from(stopped: StreamingPullManagerGracefullyStopped) -> Self {
        Self::Gracefully(stopped)
    }
}

impl From<StreamingPullManagerHardStopped> for StreamingPullManagerStopped {
    fn from(stopped: StreamingPullManagerHardStopped) -> Self {
        Self::Hard(stopped)
    }
}

impl StreamingPullManagerStoppingHandle {
    pub fn client_id(&self) -> uuid::Uuid {
        self.client_id
    }

    pub fn subscription(&self) -> &PubSubSubscription {
        &self.subscription
    }

    /// Attempts to hard-stop the manager if the graceful stop does not complete after `graceful_stop_timeout`
    /// has elapsed.
    ///
    /// See [`StreamingPullManagerStoppingHandle::hard_stop_now`] for more details.
    pub async fn hard_stop_after(
        mut self,
        graceful_stop_timeout: Duration,
    ) -> Result<StreamingPullManagerStopped, StreamingPullManagerPanicError> {
        tracing::warn!(
            {subscription = %self.subscription, client_id = %self.client_id},
            "streaming pull manager hard stop requested with a graceful stop timeout of {:?}",
            graceful_stop_timeout
        );

        let stop_status = match tokio::time::timeout(graceful_stop_timeout, self.join()).await {
            Ok(join_result) => StreamingPullManagerStopped::Gracefully(join_result?),
            Err(_) => self.hard_stop_now().await?,
        };

        Ok(stop_status)
    }

    /// Attempts to hard stop the streaming pull manager immediately.
    ///
    /// A hard stop will abort the task that is running the manager, causing it to immediately stop.
    pub async fn hard_stop_now(
        self,
    ) -> Result<StreamingPullManagerStopped, StreamingPullManagerPanicError> {
        let join_handle = match self.state {
            HandleState::Running(abort_on_drop) => abort_on_drop.into_inner(),
            HandleState::GracefullyStopped(instant) => {
                return Ok(StreamingPullManagerGracefullyStopped::new(
                    self.graceful_stop_initiated_at,
                    instant,
                )
                .into());
            }
            HandleState::PreviouslyPanicked => {
                return Err(StreamingPullManagerPanicError::PreviouslyPanicked)
            }
        };

        tracing::warn!(
            {subscription = %self.subscription, client_id = %self.client_id},
            "streaming pull hard stopping now, aborting manager task",
        );

        let abort_started_at = Instant::now();
        join_handle.abort();

        let join_error = match join_handle.await {
            // The task had already completed successfully.
            Ok(instant) => {
                tracing::warn!(
                    {subscription = %self.subscription, client_id = %self.client_id},
                    "streaming pull manager task was aborted, however, it had completed prior to the abort request being processed",
                );

                return Ok(StreamingPullManagerGracefullyStopped::new(
                    self.graceful_stop_initiated_at,
                    instant,
                )
                .into());
            }
            // The task may have panicked, or successfully aborted itself.
            Err(join_error) => join_error,
        };

        match join_error.try_into_panic() {
            Ok(panic) => Err(StreamingPullManagerPanicError::Panicked { panic }),
            // The join error is due to us calling `join_handle.abort()`.
            Err(_) => {
                let instant = Instant::now();
                let abort_duration = instant - abort_started_at;
                let hard = StreamingPullManagerHardStopped {
                    duration: self.graceful_stop_initiated_at.elapsed(),
                    abort_duration,
                    instant,
                };

                tracing::warn!(
                    {subscription = %self.subscription, client_id = %self.client_id},
                    "streaming pull manager task was successfully hard stopped, took {:?} since graceful start, and {:?} to process the hard stop",
                    hard.duration, hard.abort_duration
                );

                Ok(hard.into())
            }
        }
    }

    /// Waits for the manager to gracefully stop.
    ///
    /// The manager may panic before then however, in which case an error is returned.
    ///
    /// # Cancel Safety:
    ///
    /// This function is cancel safe, and will not lose any error states if the future is cancelled prior to completion.
    pub async fn join(
        &mut self,
    ) -> Result<StreamingPullManagerGracefullyStopped, StreamingPullManagerPanicError> {
        self.state.join().await.map(|instant| {
            StreamingPullManagerGracefullyStopped::new(self.graceful_stop_initiated_at, instant)
        })
    }
}
