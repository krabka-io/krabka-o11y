use super::{
    AnnotatedQueryResult, ApiError, Arc, FrontendRangeRequest, HeaderMap, IntoResponse,
    MetricStore, Principal, PrometheusApiState, RangeQueryParams, Response, StdDurationExt,
    apply_result_limit, authorized_tenant_from_headers, check_range_resolution, duration_param,
    enforce_query_range_limit, execute_range_query_frontend, success_response, timestamp_ms,
    validate_timestamp_range,
};

pub(crate) async fn query_range_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    principal: &Principal,
    params: RangeQueryParams,
) -> Response {
    let tenant = match authorized_tenant_from_headers(headers, principal) {
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
    if let Err(error) = enforce_query_range_limit(state, &tenant, start_ms, end_ms) {
        return error.into_response();
    }

    // Time the pure range eval (through the frontend cache/split when enabled),
    // labelled `type="range"`; the whole-handler span stays on
    // `query_duration{route="query_range"}`.
    let eval_started = std::time::Instant::now();
    let outcome = if let Some(frontend) = &state.query_frontend {
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
            .query_range_with_annotations(&tenant, &params.query, start_ms, end_ms, step)
            .await
            .map(|(result, annotations)| AnnotatedQueryResult {
                result,
                annotations,
            })
    };
    state.record_eval("range", outcome.is_ok(), eval_started.elapsed().as_time());

    match outcome {
        Ok(AnnotatedQueryResult {
            mut result,
            annotations,
        }) => {
            apply_result_limit(&mut result, params.limit);
            success_response(result, &annotations)
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
