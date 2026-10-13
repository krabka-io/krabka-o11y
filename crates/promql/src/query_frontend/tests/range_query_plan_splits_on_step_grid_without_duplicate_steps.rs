use super::*;

#[derive(Debug, Eq, PartialEq)]
struct PlannedRangeCase {
    start_ms: i64,
    end_ms: i64,
    shard: Option<QueryShard>,
}

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
        .map(|subquery| PlannedRangeCase {
            start_ms: subquery.start_ms,
            end_ms: subquery.end_ms,
            shard: subquery.shard,
        })
        .collect::<Vec<_>>();

    // Eval points 0, 60k, 120k, 180k, 240k bucket into absolute
    // split-interval windows [0,120k), [120k,240k), [240k,360k); each
    // sub-range spans the eval points landing in its absolute window with
    // no duplicate step across sub-ranges.
    assert2::assert!(
        ranges
            == vec![
                PlannedRangeCase {
                    start_ms: 0,
                    end_ms: 60_000,
                    shard: None,
                },
                PlannedRangeCase {
                    start_ms: 120_000,
                    end_ms: 180_000,
                    shard: None,
                },
                PlannedRangeCase {
                    start_ms: 240_000,
                    end_ms: 240_000,
                    shard: None,
                },
            ]
    );
}
