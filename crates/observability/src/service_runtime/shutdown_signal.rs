/// Waits for an operating-system signal asking the process to stop.
///
/// `SIGTERM` -- what Kubernetes, systemd and `docker stop` send -- and
/// `SIGINT` -- what Ctrl+C sends -- both resolve the future. Loki, Mimir and
/// Tempo trap the two identically and run the same stop chain on either, and
/// so does every role here: a role that heard only `SIGINT` would be killed at
/// the end of the orchestrator's grace period, losing whatever it had not yet
/// flushed and committed.
///
/// A handler that cannot be installed also resolves the future. A process that
/// cannot hear a stop request should stop rather than run on unstoppable.
///
/// The stack is Unix-only -- the Bazel toolchains list Linux and macOS, CI
/// runs on Linux, and `krabka-pprof` needs Linux -- so this needs no fallback
/// for a platform without `SignalKind`.
pub async fn shutdown_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install SIGINT handler; triggering shutdown");
        }
    };

    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler; triggering shutdown");
            }
        }
    };

    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }
}
