use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_stdvar_from_moment_partials() {
    let cache = QueryFrontendCache::default();
    let executor = MomentPartialRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "stdvar(up)".into(),
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
        .expect("moment partial executor calls poisoned")
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
                ("sum((up) * (up))", Some(QueryShard { index: 1, total: 2 }),),
                ("sum((up) * (up))", Some(QueryShard { index: 2, total: 2 }),),
            ]
    );
    let QueryResult::RangeMatrix(series) = result else {
        panic!("stdvar range matrix");
    };
    let SampleValue::Float(value) = series[0].samples[0].1 else {
        panic!("stdvar float sample");
    };
    assert2::assert!((value - (38.0 / 3.0)).abs() < 1e-9);
}
