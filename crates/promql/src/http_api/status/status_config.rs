use krabka_units::fmt::Human as _;

use super::{Arc, MetricStore, PrometheusApiState, Response, State, json, success_data_response};

pub(crate) async fn status_config<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
) -> Response {
    let retention = state.storage_retention.map_or_else(
        || "unknown (metric store does not expose retention)".to_string(),
        |retention| retention.human().to_string(),
    );
    success_data_response(json!({
        "yaml": format!(
            "krabka:\n  scrape_config: not applicable (Krabka does not scrape)\n  query_lookback_delta: {}\n  query_max_concurrency: {}\n  storage_retention: {retention}\n",
            state.engine_opts.lookback_delta.human(),
            state.max_concurrent_queries,
        ),
    }))
}
