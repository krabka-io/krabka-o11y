use std::time::Duration;

use super::{
    AnnotatedQueryResult, ApiError, IntoResponse, QueryRequestTiming, QueryResponseStats, Response,
    apply_result_limit, success_response, success_response_with_stats,
};
use crate::{engine::QuerySampleStats, http_api::response::QueryPhaseDurations};

/// One evaluated instant or range query, with what its `stats` block reports.
pub(super) struct EvaluatedQuery {
    /// The evaluation's result and annotations, or its error.
    pub(super) outcome: crate::error::Result<AnnotatedQueryResult>,
    /// The sample counts, present only when the request asked for `stats`.
    pub(super) samples: Option<QuerySampleStats>,
    /// The time spent decoding and checking the request before evaluation.
    pub(super) preparation: Duration,
    /// The time the engine spent evaluating.
    pub(super) evaluation: Duration,
}

/// Renders an evaluated query: the limited result with its annotations, and
/// with a `stats` block when the request asked for one, or the error.
pub(super) fn evaluated_query_response(
    evaluated: EvaluatedQuery,
    limit: Option<usize>,
    timing: QueryRequestTiming,
) -> Response {
    let EvaluatedQuery {
        outcome,
        samples,
        preparation,
        evaluation,
    } = evaluated;
    match outcome {
        Ok(AnnotatedQueryResult {
            mut result,
            annotations,
        }) => {
            apply_result_limit(&mut result, limit);
            match samples {
                Some(samples) => success_response_with_stats(
                    result,
                    QueryResponseStats::new(
                        samples,
                        QueryPhaseDurations {
                            preparation,
                            evaluation,
                            queue: timing.queue,
                            total: timing.started.elapsed(),
                        },
                    ),
                    &annotations,
                ),
                None => success_response(result, &annotations),
            }
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
