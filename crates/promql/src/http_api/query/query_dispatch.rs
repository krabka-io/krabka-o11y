use super::{
    ApiError, Arc, HeaderMap, InstantQueryParams, IntoResponse, MetricStore, PrometheusApiState,
    Response, StdDurationExt, apply_result_limit, optional_timestamp_ms, success_response,
    tenant_from_headers,
};

pub(crate) async fn query_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    params: InstantQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let time_ms = match optional_timestamp_ms(params.time.as_deref()) {
        Ok(time_ms) => time_ms,
        Err(error) => return error.into_response(),
    };

    let engine = state.engine_for_tenant(&tenant);
    // Time the pure engine eval (parse+plan+execute), excluding param decode,
    // permit wait, and response encoding — that whole-handler span is already
    // covered by `query_duration{route}`.
    let eval_started = std::time::Instant::now();
    let outcome = engine.query_instant(&tenant, &params.query, time_ms).await;
    state.record_eval("instant", outcome.is_ok(), eval_started.elapsed().as_time());
    match outcome {
        Ok(mut result) => {
            apply_result_limit(&mut result, params.limit);
            success_response(result)
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
