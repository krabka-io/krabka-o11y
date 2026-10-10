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
}
