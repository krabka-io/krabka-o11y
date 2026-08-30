use super::*;

#[test]
pub(crate) fn range_query_plan_rejects_resolution_over_point_cap() {
    // (end-start)/step = 20_000 / 1 = 20_000 > 11_000: the frontend planner
    // must reject before expanding into ~20k per-step sub-queries, matching
    // Prometheus's unconditional resolution front-gate.
    let error = plan_range_query(
        "up",
        0,
        20_000,
        millis(1),
        QueryFrontendOptions {
            split_interval: millis(120_000),
            shard_count: 1,
        },
    )
    .unwrap_err();

    match error {
        PromqlError::Plan(message) => assert2::assert!(
            message
                == "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
        ),
        other => panic!("expected Plan error, got {other:?}"),
    }
}
