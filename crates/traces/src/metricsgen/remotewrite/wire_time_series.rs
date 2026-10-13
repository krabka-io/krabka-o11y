use super::{Exemplar, NativeHistogram};

/// Flat `remote_write` row, neutral to the encoder.
#[derive(Clone, Debug, PartialEq)]
pub struct WireTimeSeries {
    pub labels: Vec<krabka_traceql::MetricLabel>,
    pub value: f64,
    pub timestamp_ms: i64,
    pub exemplars: Vec<Exemplar>,
    pub native_histogram: Option<NativeHistogram>,
}
