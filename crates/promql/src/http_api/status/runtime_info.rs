use super::{
    ApiError, Arc, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, State,
    SystemTime, json, success_data_response, tenant_from_headers, unix_time_string,
};

pub(crate) async fn runtime_info<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let tsdb_stats = match state.store.tsdb_stats(&tenant).await {
        Ok(tsdb_stats) => tsdb_stats,
        Err(error) => return ApiError::from(error).into_response(),
    };

    success_data_response(json!({
        "startTime": unix_time_string(state.start_time),
        "CWD": std::env::current_dir()
            .ok()
            .and_then(|path| path.into_os_string().into_string().ok())
            .unwrap_or_default(),
        "hostname": "",
        "serverTime": unix_time_string(SystemTime::now()),
        "reloadConfigSuccess": true,
        "lastConfigTime": unix_time_string(state.start_time),
        "timeSeriesCount": tsdb_stats.head_stats.num_series,
        "corruptionCount": 0,
        "goroutineCount": 0,
        "GOMAXPROCS": 0,
        "GOGC": "",
        "GODEBUG": "",
        "storageRetention": "",
    }))
}
