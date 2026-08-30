use super::{
    ApiError, Arc, FrontendRangeRequest, HeaderMap, IntoResponse, MetricStore, PrometheusApiState,
    QueryEnforcer, RangeQueryParams, Response, StdDurationExt, apply_result_limit,
    check_range_resolution, duration_param, execute_range_query_frontend, success_response,
    tenant_from_headers, timestamp_ms, unix_now_ms, validate_timestamp_range,
};

pub(crate) async fn query_range_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    params: RangeQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let start_ms = match timestamp_ms(&params.start) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let end_ms = match timestamp_ms(&params.end) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_timestamp_range(start_ms, end_ms) {
        return error.into_response();
    }
    let step = match duration_param(&params.step) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = check_range_resolution(start_ms, end_ms, step) {
        return error.into_response();
    }
    if let Some(limits) = &state.query_limits {
        let now_ms = match unix_now_ms() {
            Ok(now_ms) => now_ms,
            Err(error) => return error.into_response(),
        };
        if let Err(error) =
            QueryEnforcer::check_range(limits.for_tenant(&tenant), start_ms, end_ms, now_ms)
        {
            return ApiError::from(error).into_response();
        }
    }

    // Time the pure range eval (through the frontend cache/split when enabled),
    // labelled `type="range"`; the whole-handler span stays on
    // `query_duration{route="query_range"}`.
    let eval_started = std::time::Instant::now();
    let result = if let Some(frontend) = &state.query_frontend {
        let engine = state.engine_for_tenant(&tenant);
        execute_range_query_frontend(
            &engine,
            frontend.cache.as_ref(),
            &FrontendRangeRequest {
                tenant: tenant.clone(),
                query: params.query.clone(),
                start_ms,
                end_ms,
                step,
                opts: frontend.opts,
            },
        )
        .await
    } else {
        state
            .engine_for_tenant(&tenant)
            .query_range(&tenant, &params.query, start_ms, end_ms, step)
            .await
    };
    state.record_eval("range", result.is_ok(), eval_started.elapsed().as_time());

    match result {
        Ok(mut result) => {
            apply_result_limit(&mut result, params.limit);
            success_response(result)
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
