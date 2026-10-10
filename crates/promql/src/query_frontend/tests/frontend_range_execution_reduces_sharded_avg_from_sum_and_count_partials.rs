use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_avg_from_sum_and_count_partials() {
    let cache = QueryFrontendCache::default();
    let executor = AvgPartialRecordingExecutor::default();

    let result =
        execute_range_query_frontend(&executor, &cache, &two_shard_instant_request("avg(up)"))
            .await
            .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("avg partial executor calls poisoned")
        .clone();
    assert2::assert!(shard_calls(&calls) == on_both_shards(&["sum(up)", "count(up)"]));
    assert2::assert!(
        result
            == unannotated(QueryResult::RangeMatrix(vec![RangeSeries {
                drop_name: false,
                start_timestamps_ms: std::collections::BTreeMap::new(),
                labels: labels(&[]).into(),
                samples: vec![(0, SampleValue::Float(4.0))],
            }]))
    );
}
