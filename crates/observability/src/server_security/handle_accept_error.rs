use std::{io, time::Duration};

/// Reports a failed `accept`, and waits before the next one when the error is not a single connection's problem.
///
/// This copies `axum::serve`'s own `TcpListener` behaviour. A connection that
/// the client reset before the accept is not a fault. Any other error, such as
/// `EMFILE`, is logged and followed by a one-second pause, so the loop does not
/// spin while the process has no file descriptors left.
pub async fn handle_accept_error(error: io::Error) {
    if matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    ) {
        return;
    }
    tracing::error!(%error, "accept error; retrying in one second");
    tokio::time::sleep(Duration::from_secs(1)).await;
}
