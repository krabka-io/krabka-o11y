use super::*;

/// A profile limit violation with the Connect and HTTP projection Pyroscope clients expect.
///
/// The variant payloads are raw numbers and not quantities. Each one goes
/// straight into a Pyroscope-facing error string in a fixed unit: profiles per
/// second, bytes, or whole seconds. The extraction therefore happens once at
/// construction and not at every render site.
#[derive(Debug, thiserror::Error, Clone, PartialEq)]
pub enum LimitError {
    #[error("ingestion rate exceeded: observed {observed} profiles/sec above limit {rate}")]
    IngestionRateExceeded { rate: f64, observed: f64 },
    #[error("max series exceeded: observed {observed} above limit {limit}")]
    MaxSeries { limit: u64, observed: u64 },
    #[error("label name too long: observed {observed} bytes above limit {limit}")]
    LabelNameTooLong { limit: u64, observed: u64 },
    #[error("label value too long: observed {observed} bytes above limit {limit}")]
    LabelValueTooLong { limit: u64, observed: u64 },
    #[error("too many label names: observed {observed} above limit {limit}")]
    TooManyLabels { limit: u64, observed: u64 },
    #[error("query length exceeded: observed {observed_secs}s above limit {limit_secs}s")]
    QueryLengthExceeded { limit_secs: u64, observed_secs: u64 },
    #[error("session cardinality exceeded: limit {limit}")]
    SessionCardinalityExceeded { limit: u64 },
}

impl LimitError {
    #[must_use]
    pub fn connect_code(&self) -> &'static str {
        match self {
            Self::IngestionRateExceeded { .. }
            | Self::MaxSeries { .. }
            | Self::SessionCardinalityExceeded { .. } => "resource_exhausted",
            Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. }
            | Self::TooManyLabels { .. }
            | Self::QueryLengthExceeded { .. } => "invalid_argument",
        }
    }

    #[must_use]
    pub fn http_status(&self) -> u16 {
        match self {
            Self::IngestionRateExceeded { .. }
            | Self::MaxSeries { .. }
            | Self::SessionCardinalityExceeded { .. } => 429,
            Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. }
            | Self::TooManyLabels { .. }
            | Self::QueryLengthExceeded { .. } => 400,
        }
    }

    #[must_use]
    pub fn message(&self) -> String {
        self.to_string()
    }
}
