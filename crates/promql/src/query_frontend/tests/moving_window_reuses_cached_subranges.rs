use super::*;

#[tokio::test]
pub(crate) async fn moving_window_reuses_cached_subranges() {
    let opts = QueryFrontendOptions {
        split_interval: millis(120_000),
        shard_count: 1,
    };
    let cache = QueryFrontendCache::default();
    let executor = RecordingExecutor::default();

    // First query window [0, 360_000].
    execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "up".into(),
            start_ms: 0,
            end_ms: 360_000,
            step: millis(60_000),
            opts,
        },
    )
    .await
    .unwrap();

    let first_fresh = executor
        .calls
        .lock()
        .expect("recording executor calls poisoned")
        .len();
    // Absolute buckets: [0,120k)->[0,60k], [120k,240k)->[120k,180k],
    // [240k,360k)->[240k,300k], [360k,480k)->[360k,360k] => 4 sub-queries.
    assert2::assert!(first_fresh == 4);

    // Second window shifted by one step (60_000 < split 120_000) and the
    // same step phase, so the absolute-aligned interior buckets
    // [120k,240k) and [240k,360k) reproduce identical sub-ranges that are
    // already cached.
    execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "up".into(),
            start_ms: 60_000,
            end_ms: 420_000,
            step: millis(60_000),
            opts,
        },
    )
    .await
    .unwrap();

    let all_calls = executor
        .calls
        .lock()
        .expect("recording executor calls poisoned")
        .clone();
    let second_fresh = all_calls.len() - first_fresh;

    // Second window sub-ranges: [60k,60k] | [120k,180k]* | [240k,300k]* |
    // [360k,420k]. The two starred interior sub-ranges hit the cache, so
    // only the two non-cached sub-ranges execute fresh.
    assert2::assert!(second_fresh == 2);
    let second_starts = all_calls[first_fresh..]
        .iter()
        .map(|query| (query.start_ms, query.end_ms))
        .collect::<Vec<_>>();
    assert2::assert!(second_starts == vec![(60_000, 60_000), (360_000, 420_000)]);
}
