use super::*;

#[tokio::test]
pub(crate) async fn a_cached_range_query_reports_the_annotations_of_the_miss() {
    let request = FrontendRangeRequest {
        tenant: tenant_id("tenant-a"),
        query: "up".into(),
        start_ms: 0,
        end_ms: 360_000,
        step: millis(60_000),
        opts: QueryFrontendOptions {
            split_interval: millis(120_000),
            shard_count: 1,
        },
    };
    let cache = QueryFrontendCache::default();
    let executor = WarningExecutor::default();

    let miss = execute_range_query_frontend(&executor, &cache, &request)
        .await
        .unwrap();
    let fresh_calls = executor.call_count();
    assert2::assert!(fresh_calls > 1);

    let hit = execute_range_query_frontend(&executor, &cache, &request)
        .await
        .unwrap();

    // The second request ran no sub-query, so every annotation it reports came
    // out of the cache.
    assert2::check!(executor.call_count() == fresh_calls);
    assert2::check!(
        hit.annotations
            == Annotations {
                warnings: vec![
                    "PromQL warning: block metrics/float/0001.parquet is missing".to_string()
                ],
                infos: vec!["PromQL info: metric might not be a counter".to_string()],
            }
    );
    assert2::check!(hit == miss);
}
