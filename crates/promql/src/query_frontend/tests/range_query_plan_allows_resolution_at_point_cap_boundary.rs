use super::*;

#[test]
pub(crate) fn range_query_plan_allows_resolution_at_point_cap_boundary() {
    // (end-start)/step = 11_000 / 1 = 11_000 is allowed (cap is exclusive,
    // matching Prometheus's `> 11000`).
    let plan = plan_range_query(
        "up",
        0,
        11_000,
        millis(1),
        QueryFrontendOptions {
            split_interval: millis(120_000),
            shard_count: 1,
        },
    )
    .unwrap();
    assert2::assert!(!plan.is_empty());
}
