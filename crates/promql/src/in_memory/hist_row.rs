use std::sync::Arc;

use super::{Labels, NativeHistogram, SeriesFingerprint};

#[derive(Clone)]
pub(crate) struct HistRow {
    pub(crate) fp: SeriesFingerprint,
    /// Shared, and cloned by pointer when the head is copied.
    pub(crate) labels: Arc<Labels>,
    pub(crate) ts_ms: i64,
    /// Behind an `Arc` for the same reason as `labels`: a histogram owns four
    /// vectors, and copying them on every head clone is what this avoids.
    pub(crate) hist: Arc<NativeHistogram>,
}
