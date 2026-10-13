use super::{Exemplar, SeriesSample};

/// One named Prometheus series without the `__name__` label.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    pub name: String,
    pub labels: Vec<krabka_traceql::MetricLabel>,
    pub sample: SeriesSample,
    pub exemplars: Vec<Exemplar>,
    pub timestamp_ms: i64,
}
