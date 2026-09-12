use super::{Deserialize, Serialize};

/// Metric block payload kind used in deterministic object keys.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MetricBlockKind {
    Float,
    NativeHistograms,
    Exemplars,
    Metadata,
    ClockReadings,
}

impl MetricBlockKind {
    pub(crate) const fn object_path(self) -> &'static str {
        match self {
            Self::Float => "float",
            Self::NativeHistograms => "native-histograms",
            Self::Exemplars => "exemplars",
            Self::Metadata => "metadata",
            Self::ClockReadings => "clock-readings",
        }
    }

    /// Whether a level compaction may merge two blocks of this kind into one.
    ///
    /// Every kind is mergeable. Float and native-histogram rows are
    /// deduplicated by their real `(fingerprint, timestamp)` key. The other
    /// kinds are merged without key deduplication: exemplars may legitimately
    /// share that pair, metadata is already set-deduplicated by its read path,
    /// and clock readings are an archival record beside their queryable float
    /// projection.
    pub(crate) const fn is_mergeable(self) -> bool {
        true
    }

    /// Whether repeated `(fingerprint, timestamp)` rows are duplicates.
    pub(crate) const fn deduplicates_series_timestamp(self) -> bool {
        matches!(self, Self::Float | Self::NativeHistograms)
    }
}
