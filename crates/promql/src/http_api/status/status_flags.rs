use super::*;

pub(crate) async fn status_flags<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
) -> Response {
    let retention = state.storage_retention.map_or_else(
        || "unknown (metric store does not expose retention)".to_string(),
        |retention| retention.human().to_string(),
    );
    success_data_response(json!({
        "log.level": state.status_log_level.as_str(),
        "query.lookback-delta": state.engine_opts.lookback_delta.human().to_string(),
        "query.max-concurrency": state.max_concurrent_queries.to_string(),
        "storage.tsdb.retention.time": retention,
    }))
}
