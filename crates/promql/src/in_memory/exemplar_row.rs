use super::*;

#[derive(Clone)]
pub(crate) struct ExemplarRow {
    pub(crate) series_labels: Labels,
    pub(crate) labels: Labels,
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
}
