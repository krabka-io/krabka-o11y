use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_stddev_from_moment_partials() {
    let cache = QueryFrontendCache::default();
    let executor = MomentPartialRecordingExecutor::default();

    let result =
        execute_range_query_frontend(&executor, &cache, &two_shard_instant_request("stddev(up)"))
            .await
            .unwrap();

    let QueryResult::RangeMatrix(series) = result.result else {
        panic!("stddev range matrix");
    };
    let SampleValue::Float(value) = series[0].samples[0].1 else {
        panic!("stddev float sample");
    };
    assert2::assert!((value - (38.0_f64 / 3.0).sqrt()).abs() < 1e-9);
}
