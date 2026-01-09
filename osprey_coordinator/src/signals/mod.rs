/// Returns a future which will resolve when Ctrl-C is received.
///
/// Useful to know when the process should begin its cleanup and graceful shutdown.
#[cfg(windows)]
pub async fn exit_signal() {
    tokio::signal::ctrl_c().await;
}

/// Returns a future which will resolve when SIGINT/SIGTERM are sent to the process.
///
/// Useful to know when the process should begin its cleanup and graceful shutdown.
#[cfg(unix)]
pub async fn exit_signal() {
    use tokio::signal::unix::{Signal, SignalKind};

    let mut term = create_signal(SignalKind::terminate());
    let mut ctrl_c = create_signal(SignalKind::interrupt());

    tokio::select! {
        _ = term.recv() => {},
        _ = ctrl_c.recv() => {},
    }

    fn create_signal(kind: SignalKind) -> Signal {
        tokio::signal::unix::signal(kind).expect("couldn't create signal.")
    }
}
