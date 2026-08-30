use super::*;

pub(crate) async fn status_flags<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
) -> Response {
    success_data_response(json!({
        "log.level": "info",
        "query.lookback-delta": state.engine_opts.lookback_delta.human().to_string(),
        "query.max-concurrency": state.max_concurrent_queries.to_string(),
        "storage.tsdb.retention.time": "15d",
    }))
}
