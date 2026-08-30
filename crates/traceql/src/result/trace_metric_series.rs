use super::TraceMetricExemplar;

/// One `TraceQL` metrics series.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricSeries {
    pub labels: Vec<(String, String)>,
    pub points: Vec<(i64, f64)>,
    pub exemplars: Vec<TraceMetricExemplar>,
}
