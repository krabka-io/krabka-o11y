use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_avg_from_sum_and_count_partials() {
    let cache = QueryFrontendCache::default();
    let executor = AvgPartialRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: tenant_id("tenant-a"),
            query: "avg(up)".into(),
            start_ms: 0,
            end_ms: 0,
            step: millis(60_000),
            admission_limits: krabka_query_frontend::AdmissionLimits::default(),
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
