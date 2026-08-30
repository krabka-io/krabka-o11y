use super::Shutdown;

/// Spawn a task that sets the shared shutdown on the first shutdown signal.
///
/// One signal stops the server and all background tasks together.
pub(crate) fn spawn_shutdown_signal_listener(shutdown: Shutdown) {
    tokio::spawn(async move {
        krabka_observability::shutdown_signal().await;
        shutdown.trigger();
    });
}
