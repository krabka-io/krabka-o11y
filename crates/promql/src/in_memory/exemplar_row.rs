use std::sync::Arc;

use super::Labels;

#[derive(Clone)]
pub(crate) struct ExemplarRow {
    /// Shared, and cloned by pointer when the head is copied.
    pub(crate) series_labels: Arc<Labels>,
    pub(crate) labels: Arc<Labels>,
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
}
