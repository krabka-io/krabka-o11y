use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_avg_from_sum_and_count_partials() {
    let cache = QueryFrontendCache::default();
    let executor = AvgPartialRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "avg(up)".into(),
            start_ms: 0,
            end_ms: 0,
            step: millis(60_000),
            opts: QueryFrontendOptions {
                split_interval: millis(60_000),
                shard_count: 2,
            },
        },
    )
    .await
    .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("avg partial executor calls poisoned")
        .clone();
    assert2::assert!(
        calls
            .iter()
            .map(|query| (query.query.as_str(), query.shard))
            .collect::<Vec<_>>()
            == vec![
                ("sum(up)", Some(QueryShard { index: 1, total: 2 })),
                ("sum(up)", Some(QueryShard { index: 2, total: 2 })),
                ("count(up)", Some(QueryShard { index: 1, total: 2 })),
                ("count(up)", Some(QueryShard { index: 2, total: 2 })),
            ]
    );
    assert2::assert!(
        result
            == QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[]),
                samples: vec![(0, SampleValue::Float(4.0))],
            }])
    );
}
