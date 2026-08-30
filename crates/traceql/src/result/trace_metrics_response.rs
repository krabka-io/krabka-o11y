use super::TraceMetricSeries;

/// `TraceQL` metrics response.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricsResponse {
    pub series: Vec<TraceMetricSeries>,
}
