use super::*;

/// One Prometheus-style exemplar attached to a `TraceQL` metrics series.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricExemplar {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp_ns: i64,
}
