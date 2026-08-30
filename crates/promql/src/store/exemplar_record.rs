use super::Labels;

/// One exemplar attached to a metric series.
#[derive(Clone, Debug, PartialEq)]
pub struct ExemplarRecord {
    pub series_labels: Labels,
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}
