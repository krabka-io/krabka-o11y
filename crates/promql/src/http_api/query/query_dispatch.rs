use krabka_observability::service_metrics::RequestOutcome;

use super::{
    Arc, EvaluatedQuery, HeaderMap, InstantQueryParams, IntoResponse, MetricStore,
    PerStepSampleStats, Principal, PrometheusApiState, QueryDispatchRequest, QueryRequestTiming,
    Response, StdDurationExt, StepGrid, authorized_query_tenant, collect_query_sample_stats,
    enforce_query_range_limit, evaluated_query_response, optional_timestamp_ms, query_stats_step,
};

pub(crate) async fn query_dispatch<S: MetricStore>(
    request: QueryDispatchRequest<'_, S, InstantQueryParams>,
) -> Response {
    let preparation_started = std::time::Instant::now();
    let tenant = match authorized_query_tenant(request.headers, request.principal) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let time_ms = match optional_timestamp_ms(request.params.time.as_deref()) {
        Ok(time_ms) => time_ms,
        Err(error) => return error.into_response(),
    };
    // Mimir reads an instant query as a range whose start and end are both the
    // evaluation time, so the lookback cap applies and the range-length cap
    // passes.
    if let Err(error) = enforce_query_range_limit(request.state, &tenant, time_ms, time_ms) {
        return error.into_response();
    }

    let engine = request.state.engine_for_tenant(&tenant);
    let stats_requested = request
        .params
        .stats
        .as_deref()
        .is_some_and(|stats| !stats.is_empty());
    let per_step_stats = request.params.stats.as_deref() == Some("all");
    let preparation = preparation_started.elapsed();
    // Time the pure engine eval (parse+plan+execute), excluding param decode,
    // permit wait, and response encoding — that whole-handler span is already
    // covered by `query_duration{route}`.
    let eval_started = std::time::Instant::now();
    let mut evaluated = if stats_requested {
        let per_step = if per_step_stats {
            PerStepSampleStats::Include(StepGrid::instant(time_ms, 1))
        } else {
            PerStepSampleStats::Omit
        };
        let query = collect_query_sample_stats(
            per_step,
            query_stats_step(
                time_ms,
                engine.query_instant_with_annotations(&tenant, &request.params.query, time_ms),
            ),
        )
        .await;
        EvaluatedQuery {
            outcome: query.output,
            samples: Some(query.stats),
            preparation,
            evaluation: std::time::Duration::ZERO,
        }
    } else {
        EvaluatedQuery {
            outcome: engine
                .query_instant_with_annotations(&tenant, &request.params.query, time_ms)
                .await,
            samples: None,
            preparation,
            evaluation: std::time::Duration::ZERO,
        }
    };
    super::finish_query_evaluation(
        request.state,
        "instant",
        evaluated,
        eval_started,
        request.params.limit,
        request.timing,
    )
}
