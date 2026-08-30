/// Failure modes of a single backend job.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend job timed out")]
    Timeout,
    #[error("backend transport error: {0}")]
    Transport(String),
    #[error("backend returned error ({status}): {message}")]
    Backend { status: String, message: String },
}

impl BackendError {
    /// Map this backend failure to the `(status, body)` the frontend returns to
    /// its client.
    ///
    /// This keeps the upstream querier's status code and error text where they
    /// are known. A timeout becomes `504`, and a transport failure becomes
    /// `502`.
    #[must_use]
    pub fn to_http(&self) -> (u16, String) {
        match self {
            BackendError::Timeout => (504, "backend job timed out".to_string()),
            BackendError::Transport(detail) => (502, format!("backend transport error: {detail}")),
            BackendError::Backend { status, message } => {
                (status.parse::<u16>().unwrap_or(502), message.clone())
            }
        }
    }
}
