use super::{
    ApiError, Arc, HeaderMap, InstantQueryParams, IntoResponse, MetricStore, Principal,
    PrometheusApiState, Response, StdDurationExt, apply_result_limit,
    authorized_tenant_from_headers, enforce_query_range_limit, optional_timestamp_ms,
    success_response,
};

pub(crate) async fn query_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    principal: &Principal,
    params: InstantQueryParams,
) -> Response {
    let tenant = match authorized_tenant_from_headers(headers, principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let time_ms = match optional_timestamp_ms(params.time.as_deref()) {
        Ok(time_ms) => time_ms,
        Err(error) => return error.into_response(),
    };
    // Mimir reads an instant query as a range whose start and end are both the
    // evaluation time, so the lookback cap applies and the range-length cap
    // passes.
    if let Err(error) = enforce_query_range_limit(state, &tenant, time_ms, time_ms) {
        return error.into_response();
    }

    let engine = state.engine_for_tenant(&tenant);
    // Time the pure engine eval (parse+plan+execute), excluding param decode,
    // permit wait, and response encoding — that whole-handler span is already
    // covered by `query_duration{route}`.
    let eval_started = std::time::Instant::now();
    let outcome = engine
        .query_instant_with_annotations(&tenant, &params.query, time_ms)
        .await;
    state.record_eval("instant", outcome.is_ok(), eval_started.elapsed().as_time());
    match outcome {
        Ok((mut result, annotations)) => {
            apply_result_limit(&mut result, params.limit);
            success_response(result, &annotations)
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
