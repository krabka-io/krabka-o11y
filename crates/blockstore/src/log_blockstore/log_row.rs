use super::{SeriesFingerprint, StructuredMetadata};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogRow {
    pub series_fingerprint: SeriesFingerprint,
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: StructuredMetadata,
}

impl LogRow {
    #[must_use]
    pub fn new(
        series_fingerprint: SeriesFingerprint,
        timestamp_ns: i64,
        line: impl Into<String>,
        structured_metadata: StructuredMetadata,
    ) -> Self {
        Self {
            series_fingerprint,
            timestamp_ns,
            line: line.into(),
            structured_metadata,
        }
    }
}
