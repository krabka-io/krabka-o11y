use super::{
    ApiError, ERASURE_REQUEST_PREFIX, EvaluatedQuery, FrontendRangeRequest, IntoResponse,
    MetricStore, PerStepSampleStats, QueryDispatchRequest, RangeQueryParams, Response, StepGrid,
    TimeExt, authorized_query_tenant, check_range_resolution, collect_query_sample_stats,
    duration_param, enforce_query_range_limit, execute_range_query_frontend, has_erasure_requests,
    timestamp_ms, validate_timestamp_range,
};

pub(crate) async fn query_range_dispatch<S: MetricStore>(
    request: QueryDispatchRequest<'_, S, RangeQueryParams>,
) -> Response {
    let QueryDispatchRequest {
        state,
        headers,
        principal,
        params,
        timing,
    } = request;
    let preparation_started = std::time::Instant::now();
    let tenant = match authorized_query_tenant(headers, principal) {
        Ok(tenant) => tenant,
        Err(response) => return response,
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
        }
    };
    let evaluated = if stats_requested {
        let per_step = if per_step_stats {
            PerStepSampleStats::Include(StepGrid {
                start: start_ms,
                end: end_ms,
                step: step.millis_i64(),
            })
        } else {
            PerStepSampleStats::Omit
        };
        let query = collect_query_sample_stats(per_step, evaluate).await;
        EvaluatedQuery {
            outcome: query.output,
            samples: Some(query.stats),
            preparation,
            evaluation: std::time::Duration::ZERO,
        }
    } else {
        EvaluatedQuery {
            outcome: evaluate.await,
            samples: None,
            preparation,
            evaluation: std::time::Duration::ZERO,
        }
    };
    super::finish_query_evaluation(
        state,
        "range",
        evaluated,
        eval_started,
        params.limit,
        timing,
    )
}
