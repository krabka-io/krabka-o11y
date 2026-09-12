use std::sync::Arc;

use super::{Labels, SeriesFingerprint};

#[derive(Clone)]
pub(crate) struct FloatRow {
    pub(crate) fp: SeriesFingerprint,
    /// Shared with every other row of the same series in the record that
    /// produced it, and cloned by pointer when the head is copied.
    pub(crate) labels: Arc<Labels>,
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
    pub(crate) start_timestamp_ms: Option<i64>,
}
