use super::*;

/// Per-surface limit failures with Prometheus and Mimir status metadata.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum LimitError {
    #[error("ingestion rate exceeded: observed {observed} samples/sec above limit {rate}")]
    IngestionRateExceeded { rate: f64, observed: f64 },
    #[error("maximum active series per user exceeded: observed {observed} above limit {limit}")]
    MaxSeriesPerUser { limit: u64, observed: u64 },
    #[error("label name too long: observed {observed} bytes above limit {limit}")]
    LabelNameTooLong { limit: u64, observed: u64 },
    #[error("label value too long: observed {observed} bytes above limit {limit}")]
    LabelValueTooLong { limit: u64, observed: u64 },
    #[error("samples per query exceeded: observed {observed} above limit {limit}")]
    SamplesPerQueryExceeded { limit: u64, observed: u64 },
    #[error("series per query exceeded: observed {observed} above limit {limit}")]
    SeriesPerQueryExceeded { limit: u64, observed: u64 },
    #[error("query lookback exceeded: observed {observed_secs}s above limit {limit_secs}s")]
    QueryLookbackExceeded { limit_secs: u64, observed_secs: u64 },
    #[error("query range too long: observed {observed_secs}s above limit {limit_secs}s")]
    QueryRangeTooLong { limit_secs: u64, observed_secs: u64 },
}

impl LimitError {
    #[must_use]
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::IngestionRateExceeded { .. } => 429,
            Self::MaxSeriesPerUser { .. }
            | Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. } => 400,
            Self::SamplesPerQueryExceeded { .. }
            | Self::SeriesPerQueryExceeded { .. }
            | Self::QueryLookbackExceeded { .. }
            | Self::QueryRangeTooLong { .. } => 422,
        }
    }

    #[must_use]
    pub const fn error_type(&self) -> &'static str {
        match self {
            Self::SamplesPerQueryExceeded { .. }
            | Self::SeriesPerQueryExceeded { .. }
            | Self::QueryLookbackExceeded { .. }
            | Self::QueryRangeTooLong { .. } => "execution",
            Self::IngestionRateExceeded { .. }
            | Self::MaxSeriesPerUser { .. }
            | Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. } => "bad_data",
        }
    }

    #[must_use]
    pub fn message(&self) -> String {
        self.to_string()
    }
}
