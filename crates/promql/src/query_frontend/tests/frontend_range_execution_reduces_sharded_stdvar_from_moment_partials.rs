use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_stdvar_from_moment_partials() {
    let cache = QueryFrontendCache::default();
    let executor = MomentPartialRecordingExecutor::default();

    let result =
        execute_range_query_frontend(&executor, &cache, &two_shard_instant_request("stdvar(up)"))
            .await
            .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("moment partial executor calls poisoned")
        .clone();
    assert2::assert!(
        shard_calls(&calls) == on_both_shards(&["sum(up)", "count(up)", "sum((up) * (up))"])
    );
    let QueryResult::RangeMatrix(series) = result.result else {
        panic!("stdvar range matrix");
    };
    let SampleValue::Float(value) = series[0].samples[0].1 else {
        panic!("stdvar float sample");
    };
    assert2::assert!((value - (38.0 / 3.0)).abs() < 1e-9);
}
