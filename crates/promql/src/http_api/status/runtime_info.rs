use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    Response, State, SystemTime, authorized_tenant_from_headers, json, success_data_response,
    unix_time_string,
};

pub(crate) async fn runtime_info<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let tsdb_stats = match state.store.tsdb_stats(tenant.as_str()).await {
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
