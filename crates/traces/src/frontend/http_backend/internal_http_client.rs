use super::{BackendError, Duration, InternalClient};

/// Builds a reqwest client for frontend-to-querier calls.
///
/// `timeout` bounds one request. `internal_client` supplies the token, TLS
/// identity, and CA bundle of the frontend's internal principal.
///
/// # Errors
/// Returns `BackendError::Transport` if the client cannot be built.
pub(crate) fn internal_http_client(
    timeout: Duration,
    internal_client: &InternalClient,
) -> Result<reqwest::Client, BackendError> {
    internal_client
        .apply(reqwest::Client::builder().timeout(timeout))
        .build()
        .map_err(|e| BackendError::Transport(e.to_string()))
}
