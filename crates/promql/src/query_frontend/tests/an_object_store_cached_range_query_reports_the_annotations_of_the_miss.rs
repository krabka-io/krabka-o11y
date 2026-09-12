use super::*;

#[tokio::test]
pub(crate) async fn an_object_store_cached_range_query_reports_the_annotations_of_the_miss() {
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
    let object_store = Arc::new(object_store::memory::InMemory::new());
    let executor = WarningExecutor::default();

    // A second cache instance over the same objects, so the hit is served from
    // the stored payload and not from any in-process state.
    let writer = ObjectStoreQueryFrontendCache::new(object_store.clone(), "query-cache");
    let miss = execute_range_query_frontend(&executor, &writer, &request)
        .await
        .unwrap();
    let fresh_calls = executor.call_count();

    let reader = ObjectStoreQueryFrontendCache::new(object_store, "query-cache");
    let hit = execute_range_query_frontend(&executor, &reader, &request)
        .await
        .unwrap();

    assert2::check!(executor.call_count() == fresh_calls);
    assert2::check!(hit == miss);
    assert2::check!(
        hit.annotations
            == Annotations {
                warnings: vec![
                    "PromQL warning: block metrics/float/0001.parquet is missing".to_string()
                ],
                infos: vec!["PromQL info: metric might not be a counter".to_string()],
            }
    );
}
