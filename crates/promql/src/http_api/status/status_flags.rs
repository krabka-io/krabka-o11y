use super::*;

pub(crate) async fn status_flags<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
) -> Response {
    let retention = super::storage_retention(&state);
    success_data_response(json!({
        "log.level": state.status_log_level.as_str(),
        "query.lookback-delta": state.engine_opts.lookback_delta.human().to_string(),
        "query.max-concurrency": state.max_concurrent_queries.to_string(),
        "storage.tsdb.retention.time": retention,
    }))
}
