use super::{
    AnnotatedQueryResult, ApiError, Arc, ERASURE_REQUEST_PREFIX, FrontendRangeRequest, HeaderMap,
    IntoResponse, MetricStore, Principal, PrometheusApiState, QueryRequestTiming,
    QueryResponseStats, RangeQueryParams, Response, StdDurationExt, TimeExt, apply_result_limit,
    authorized_tenant_from_headers, check_range_resolution, collect_query_sample_stats,
    duration_param, enforce_query_range_limit, execute_range_query_frontend, has_erasure_requests,
    success_response, success_response_with_stats, timestamp_ms, validate_timestamp_range,
};

pub(crate) async fn query_range_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    principal: &Principal,
    params: RangeQueryParams,
    timing: QueryRequestTiming,
) -> Response {
    let preparation_started = std::time::Instant::now();
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

    let stats_requested = params
        .stats
        .as_deref()
        .is_some_and(|stats| !stats.is_empty());
    let per_step_stats = params.stats.as_deref() == Some("all");
    // ponytail: one object-store listing per range query; add a generation
    // cache when erasure-enabled query throughput makes that measurable.
    let erasure_active = match &state.erasure_store {
        Some(store) => {
            match has_erasure_requests(store, ERASURE_REQUEST_PREFIX, tenant.as_str()).await {
                Ok(active) => active,
                Err(error) => return ApiError::internal(error.to_string()).into_response(),
            }
        }
        None => false,
    };
    let preparation = preparation_started.elapsed();
    // Time the pure range eval (through the frontend cache/split when enabled),
    // labelled `type="range"`; the whole-handler span stays on
    // `query_duration{route="query_range"}`.
    let eval_started = std::time::Instant::now();
    let evaluate = async {
        if let Some(frontend) = &state.query_frontend
            && !erasure_active
        {
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
        }
    };
    let (result, samples) = if stats_requested {
        let (result, samples) = collect_query_sample_stats(
            per_step_stats,
            start_ms,
            end_ms,
            step.millis_i64(),
            evaluate,
        )
        .await;
        (result, Some(samples))
    } else {
        (evaluate.await, None)
    };
    let evaluation = eval_started.elapsed();
    state.record_eval("range", result.is_ok(), evaluation.as_time());

    match result {
        Ok(AnnotatedQueryResult {
            mut result,
            annotations,
        }) => {
            apply_result_limit(&mut result, params.limit);
            match samples {
                Some(samples) => success_response_with_stats(
                    result,
                    QueryResponseStats::new(
                        samples,
                        preparation,
                        evaluation,
                        timing.queue,
                        timing.started.elapsed(),
                    ),
                    &annotations,
                ),
                None => success_response(result, &annotations),
            }
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
