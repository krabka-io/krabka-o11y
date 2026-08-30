use super::*;

/// `count_loki_metric_result_hot_tail_samples` counts matched ingester samples and
/// returns 0 when there is nothing to match: an `absent_over_time` query short-
/// circuits to 0, and a query whose response JSON has no `data.result`
/// array also yields 0. A replacement of the whole body with a constant
/// `1`, the mutant, would report a phantom ingester sample and skew the
/// store/ingester scan-stat split.
#[test]
pub(crate) fn count_loki_metric_result_hot_tail_samples_returns_zero_when_nothing_matches() {
    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: TimeRange::new(0, 300_000_000_000).unwrap(),
        query: StreamQuery {
            matchers: Vec::new(),
            pipeline: Vec::new(),
        },
        fingerprints: BTreeSet::new(),
        blocks: Vec::new(),
    };
    let frontier = CompactionFrontier::new(0);
    let eval_range = TimeRange::new(0, 300_000_000_000).unwrap();
    let step_ns = 60_000_000_000;

    // `absent_over_time` short-circuits to 0 regardless of the response body.
    let absent_query = parse_metric_query("absent_over_time({app=\"x\"}[5m])").unwrap();
    let absent = count_loki_metric_result_hot_tail_samples(
        &json!({ "data": { "result": [] } }),
        &plan,
        &absent_query,
        &[],
        &frontier,
        (eval_range, step_ns),
        &[],
    );
    check!(absent == 0);

    // A non-absent query with an empty hot tail and a response lacking any
    // `data.result` array matches nothing and returns 0.
    let count_query = parse_metric_query("count_over_time({app=\"x\"}[5m])").unwrap();
    let none = count_loki_metric_result_hot_tail_samples(
        &json!({}),
        &plan,
        &count_query,
        &[],
        &frontier,
        (eval_range, step_ns),
        &[],
    );
    check!(none == 0);
}
