use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_dispatches_subqueries_concurrently() {
    // 4 splits over [0, 720_000] with a 180_000 split interval and 60_000
    // step, times 1 shard => 4 independent sub-queries.
    let planned = plan_range_query(
        "up",
        0,
        720_000,
        millis(60_000),
        QueryFrontendOptions {
            split_interval: millis(180_000),
            shard_count: 1,
        },
    )
    .unwrap();
    let width = planned.len();
    assert2::assert!(width >= 2);

    let executor = ConcurrencyProbeExecutor::new(width);
    let cache = QueryFrontendCache::default();

    let results = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        execute_planned_range_queries(&executor, &cache, "tenant-a", planned.clone()),
    )
    .await
    .expect("parallel fan-out must not block on the per-subquery barrier")
    .unwrap();

    // Every planned sub-query was dispatched exactly once.
    let mut dispatched = executor
        .calls
        .lock()
        .expect("probe executor calls poisoned")
        .clone();
    dispatched.sort_by_key(|query| query.start_ms);
    let mut expected = planned.clone();
    expected.sort_by_key(|query| query.start_ms);
    assert2::assert!(dispatched == expected);

    // Stitched result is identical to a deterministic sequential merge,
    // independent of completion order.
    let stitched =
        merge_range_query_results_with_reducer(results.clone(), QueryShardReducer::First).unwrap();
    let mut sequential = Vec::new();
    for subquery in &planned {
        sequential.push(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(subquery.start_ms, SampleValue::Float(1.0))],
        }]));
    }
    let sequential_merge =
        merge_range_query_results_with_reducer(sequential, QueryShardReducer::First).unwrap();
    assert2::assert!(stitched == sequential_merge);
}
