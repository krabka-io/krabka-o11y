use super::*;

/// One named Prometheus series without the `__name__` label.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub sample: SeriesSample,
    pub exemplars: Vec<Exemplar>,
    pub timestamp_ms: i64,
}
