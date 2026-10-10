use std::time::Duration;

use krabka_units::prelude::*;
use serde::Serialize;

use super::{Annotations, QueryResult, Response, annotated_success_response, result_json};
use crate::engine::QuerySampleStats;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryTimings {
    #[serde(rename = "evalTotalTime")]
    eval_total: f64,
    #[serde(rename = "resultSortTime")]
    result_sort: f64,
    #[serde(rename = "queryPreparationTime")]
    query_preparation: f64,
    #[serde(rename = "innerEvalTime")]
    inner_eval: f64,
    #[serde(rename = "execQueueTime")]
    exec_queue: f64,
    #[serde(rename = "execTotalTime")]
    exec_total: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QuerySamples {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    total_queryable_samples_per_step: Vec<(f64, u64)>,
    total_queryable_samples: u64,
    peak_samples: usize,
}

/// How long each phase of one query took, as its `stats` timings report it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryPhaseDurations {
    /// Parameter decode and planning before the engine runs.
    pub(crate) preparation: Duration,
    /// The engine's evaluation.
    pub(crate) evaluation: Duration,
    /// The wait for a query permit.
    pub(crate) queue: Duration,
    /// The whole request, from receipt to response.
    pub(crate) total: Duration,
}

#[derive(Debug, Serialize)]
pub(crate) struct QueryResponseStats {
    timings: QueryTimings,
    samples: QuerySamples,
}

impl QueryResponseStats {
    pub(crate) fn new(samples: QuerySampleStats, durations: QueryPhaseDurations) -> Self {
        let QueryPhaseDurations {
            preparation,
            evaluation,
            queue,
            total,
        } = durations;
        Self {
            timings: QueryTimings {
                eval_total: evaluation.as_secs_f64(),
                // Krabka does not run a separate post-evaluation sort phase.
                result_sort: Duration::ZERO.as_secs_f64(),
                query_preparation: preparation.as_secs_f64(),
                inner_eval: evaluation.as_secs_f64(),
                exec_queue: queue.as_secs_f64(),
                exec_total: total.as_secs_f64(),
            },
            samples: QuerySamples {
                total_queryable_samples_per_step: samples
                    .per_step
                    .into_iter()
                    .map(|(timestamp_ms, count)| {
                        (Time::from_millis(timestamp_ms).secs_f64(), count)
                    })
                    .collect(),
                total_queryable_samples: samples.total_queryable_samples,
                peak_samples: samples.peak_samples,
            },
        }
    }
}

pub(crate) fn success_response_with_stats(
    result: QueryResult,
    stats: QueryResponseStats,
    annotations: &Annotations,
) -> Response {
    let mut data = result_json(result);
    data.as_object_mut()
        .expect("query result JSON is always an object")
        .insert(
            "stats".to_string(),
            serde_json::to_value(stats).expect("query stats serialize"),
        );
    annotated_success_response(data, annotations)
}
