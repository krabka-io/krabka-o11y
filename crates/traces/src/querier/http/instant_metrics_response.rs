use super::TraceMetricsResponse;

pub(crate) fn instant_metrics_response(mut resp: TraceMetricsResponse, point_ns: i64) -> TraceMetricsResponse {
    for series in &mut resp.series {
        let value = series
            .points
            .last()
            .map(|(_, value)| *value)
            .unwrap_or_default();
        series.points = vec![(point_ns, value)];
    }
    resp
}
