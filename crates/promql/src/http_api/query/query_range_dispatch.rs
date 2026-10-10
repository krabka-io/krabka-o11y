use krabka_observability::service_metrics::RequestOutcome;

use super::{
    AnnotatedQueryResult, ApiError, Arc, ERASURE_REQUEST_PREFIX, EvaluatedQuery,
    FrontendRangeRequest, HeaderMap, IntoResponse, MetricStore, PerStepSampleStats, Principal,
    PrometheusApiState, QueryRequestTiming, RangeQueryParams, Response, StdDurationExt, StepGrid,
    TimeExt, authorized_tenant_from_headers, check_range_resolution, collect_query_sample_stats,
    duration_param, enforce_query_range_limit, evaluated_query_response,
    execute_range_query_frontend, has_erasure_requests, timestamp_ms, validate_timestamp_range,
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
                    admission_limits: state
                        .query_limits
                        .for_tenant(tenant.as_str())
                        .query_admission,
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
        let per_step = if per_step_stats {
            PerStepSampleStats::Include(StepGrid {
                start: start_ms,
                end: end_ms,
                step: step.millis_i64(),
            })
        } else {
            PerStepSampleStats::Omit
        };
        let (result, samples) = collect_query_sample_stats(per_step, evaluate).await;
        (result, Some(samples))
    } else {
        (evaluate.await, None)
    };
    let evaluation = eval_started.elapsed();
    state.record_eval(
        "range",
        RequestOutcome::from_result(&result),
        evaluation.as_time(),
    );

    evaluated_query_response(
        EvaluatedQuery {
            outcome: result,
            samples,
            preparation,
            evaluation,
        },
        params.limit,
        timing,
    )
}
