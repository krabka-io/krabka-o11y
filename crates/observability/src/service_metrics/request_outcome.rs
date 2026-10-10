use axum::http::StatusCode;

/// Outcome of one ingest or query request, as the `status` label records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    /// The request succeeded: `status="ok"`.
    Ok,
    /// The request failed with any 4xx or 5xx: `status="error"`.
    Error,
}

impl RequestOutcome {
    /// The `status` label value for this outcome.
    #[must_use]
    pub const fn status(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }

    /// The outcome of a handler that answered with `status`: [`Self::Ok`]
    /// for any 2xx, [`Self::Error`] otherwise.
    #[must_use]
    pub fn from_success_status(status: StatusCode) -> Self {
        if status.is_success() {
            Self::Ok
        } else {
            Self::Error
        }
    }

    /// The outcome of a handler that returned `result`: [`Self::Ok`] for
    /// `Ok`, [`Self::Error`] for `Err`.
    #[must_use]
    pub const fn from_result<T, E>(result: &Result<T, E>) -> Self {
        match result {
            Ok(_) => Self::Ok,
            Err(_) => Self::Error,
        }
    }
}
