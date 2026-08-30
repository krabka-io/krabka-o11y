use super::*;

#[test]
pub(crate) fn range_query_plan_splits_on_step_grid_without_duplicate_steps() {
    let plan = plan_range_query(
        "rate(http_requests_total[5m])",
        0,
        250_000,
        millis(60_000),
        QueryFrontendOptions {
            split_interval: millis(120_000),
            shard_count: 1,
        },
    )
    .unwrap();

    let ranges = plan
        .iter()
        .map(|subquery| (subquery.start_ms, subquery.end_ms, subquery.shard))
        .collect::<Vec<_>>();

    // Eval points 0, 60k, 120k, 180k, 240k bucket into absolute
    // split-interval windows [0,120k), [120k,240k), [240k,360k); each
    // sub-range spans the eval points landing in its absolute window with
    // no duplicate step across sub-ranges.
    assert2::assert!(
        ranges
            == vec![
                (0, 60_000, None),
                (120_000, 180_000, None),
                (240_000, 240_000, None),
            ]
    );
}
