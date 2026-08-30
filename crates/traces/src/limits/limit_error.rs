use super::*;

#[derive(Clone, Debug, Error, PartialEq)]
pub enum LimitError {
    #[error("ingestion rate exceeded: observed {observed} spans/sec over limit {rate}")]
    IngestionRateExceeded { rate: f64, observed: f64 },
    #[error("trace exceeds max spans per trace ({limit}): observed {observed}")]
    MaxSpansPerTrace { limit: u64, observed: u64 },
    #[error("attribute exceeds max attribute bytes ({limit}): observed {observed}")]
    AttributeTooLong { limit: u64, observed: u64 },
    #[error("search limit exceeds max traces per search ({limit}): requested {requested}")]
    TracesPerSearchExceeded { limit: u64, requested: u64 },
    #[error(
        "range specified by start and end exceeds max search duration ({limit_secs}s): observed {observed_secs}s"
    )]
    SearchDurationExceeded { limit_secs: u64, observed_secs: u64 },
}

impl LimitError {
    #[must_use]
    pub fn http_status(&self) -> u16 {
        match self {
            Self::IngestionRateExceeded { .. } => 429,
            Self::MaxSpansPerTrace { .. }
            | Self::AttributeTooLong { .. }
            | Self::TracesPerSearchExceeded { .. }
            | Self::SearchDurationExceeded { .. } => 400,
        }
    }

    /// Human-readable Tempo-style cap message. The real-Tempo suite pins the
    /// exact wording.
    #[must_use]
    pub fn message(&self) -> String {
        self.to_string()
    }
}
