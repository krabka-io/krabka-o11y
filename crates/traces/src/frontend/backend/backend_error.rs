/// Failure modes of a single backend job.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend job timed out")]
    Timeout,
    #[error("backend transport error: {0}")]
    Transport(String),
    #[error("backend returned error ({status}): {message}")]
    Backend { status: String, message: String },
    #[error("query frontend overloaded; retry after {retry_after_seconds}s")]
    Overloaded { retry_after_seconds: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendHttpError {
    pub status: u16,
    pub body: String,
}

impl BackendError {
    /// Map this backend failure to the status and body the frontend returns to
    /// its client.
    ///
    /// This keeps the upstream querier's status code and error text where they
    /// are known. A timeout becomes `504`, and a transport failure becomes
    /// `502`.
    #[must_use]
    pub fn to_http(&self) -> BackendHttpError {
        match self {
            BackendError::Timeout => BackendHttpError {
                status: 504,
                body: "backend job timed out".to_string(),
            },
            BackendError::Transport(detail) => BackendHttpError {
                status: 502,
                body: format!("backend transport error: {detail}"),
            },
            BackendError::Backend { status, message } => BackendHttpError {
                status: status.parse::<u16>().unwrap_or(502),
                body: message.clone(),
            },
            BackendError::Overloaded {
                retry_after_seconds,
            } => BackendHttpError {
                status: 429,
                body: format!("query frontend overloaded; retry after {retry_after_seconds}s"),
            },
        }
    }
}
