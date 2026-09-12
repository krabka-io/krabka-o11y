use super::{
    ApiError, Arc, HeaderMap, InstantQueryParams, IntoResponse, MetricStore, Principal,
    PrometheusApiState, QueryRequestTiming, QueryResponseStats, Response, StdDurationExt,
    apply_result_limit, authorized_tenant_from_headers, collect_query_sample_stats,
    enforce_query_range_limit, optional_timestamp_ms, query_stats_step, success_response,
    success_response_with_stats,
};

pub(crate) async fn query_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    principal: &Principal,
    params: InstantQueryParams,
    timing: QueryRequestTiming,
) -> Response {
    let preparation_started = std::time::Instant::now();
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
    let stats_requested = params
        .stats
        .as_deref()
        .is_some_and(|stats| !stats.is_empty());
    let per_step_stats = params.stats.as_deref() == Some("all");
    let preparation = preparation_started.elapsed();
    // Time the pure engine eval (parse+plan+execute), excluding param decode,
    // permit wait, and response encoding — that whole-handler span is already
    // covered by `query_duration{route}`.
    let eval_started = std::time::Instant::now();
    let (outcome, samples) = if stats_requested {
        let (outcome, samples) = collect_query_sample_stats(
            per_step_stats,
            time_ms,
            time_ms,
            1,
            query_stats_step(
                time_ms,
                engine.query_instant_with_annotations(&tenant, &params.query, time_ms),
            ),
        )
        .await;
        (outcome, Some(samples))
    } else {
        (
            engine
                .query_instant_with_annotations(&tenant, &params.query, time_ms)
                .await,
            None,
        )
    };
    let evaluation = eval_started.elapsed();
    state.record_eval("instant", outcome.is_ok(), evaluation.as_time());
    match outcome {
        Ok((mut result, annotations)) => {
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
