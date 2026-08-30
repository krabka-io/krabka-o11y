use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_reduces_sharded_stddev_from_moment_partials() {
    let cache = QueryFrontendCache::default();
    let executor = MomentPartialRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "stddev(up)".into(),
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

    let QueryResult::RangeMatrix(series) = result else {
        panic!("stddev range matrix");
    };
    let SampleValue::Float(value) = series[0].samples[0].1 else {
        panic!("stddev float sample");
    };
    assert2::assert!((value - (38.0_f64 / 3.0).sqrt()).abs() < 1e-9);
}
