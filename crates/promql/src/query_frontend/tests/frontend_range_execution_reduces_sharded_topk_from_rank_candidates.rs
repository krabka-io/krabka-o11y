use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_topk_from_rank_candidates() {
    let cache = QueryFrontendCache::default();
    let executor = RankRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "topk(2, up)".into(),
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
        .expect("rank executor calls poisoned")
        .clone();
    assert2::assert!(
        calls
            .iter()
            .map(|query| (query.query.as_str(), query.shard))
            .collect::<Vec<_>>()
            == vec![
                ("topk(2, up)", Some(QueryShard { index: 1, total: 2 })),
                ("topk(2, up)", Some(QueryShard { index: 2, total: 2 })),
            ]
    );
    let QueryResult::RangeMatrix(series) = result else {
        panic!("topk range matrix");
    };
    let selected = series
        .iter()
        .map(|series| {
            let SampleValue::Float(value) = series.samples[0].1 else {
                panic!("topk float sample");
            };
            (series.labels.get("series").unwrap().to_string(), value)
        })
        .collect::<Vec<_>>();
    assert2::assert!(selected == vec![("a".to_string(), 10.0), ("c".to_string(), 9.0)]);
}
