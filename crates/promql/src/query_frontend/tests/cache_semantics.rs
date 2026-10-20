use super::*;

mod a_cached_range_query_reports_the_annotations_of_the_miss {
    use super::*;

    #[tokio::test]
    pub(crate) async fn a_cached_range_query_reports_the_annotations_of_the_miss() {
        let request = split_up_request();
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
                    ..crate::Annotations::default()
                }
        );
        assert2::check!(hit == miss);
    }
}

mod an_object_store_cached_range_query_reports_the_annotations_of_the_miss {
    use super::*;

    #[tokio::test]
    pub(crate) async fn an_object_store_cached_range_query_reports_the_annotations_of_the_miss() {
        let request = split_up_request();
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
                    ..crate::Annotations::default()
                }
        );
    }
}

mod in_memory_round_trips_then_expires {
    use super::*;

    #[tokio::test]
    pub(crate) async fn in_memory_round_trips_then_expires() {
        let clock = Arc::new(ManualClock::new(1_000_000));
        let cache = QueryFrontendCache::with_ttl(secs(90)).with_clock(clock.clone());
        let fixture = up_query_and_result();
        let query = fixture.query;
        let result = fixture.result;

        assert_ttl_hit_then_miss(&cache, &clock, &query, &result, 89_000, 2_000).await;
    }
}

mod in_memory_without_ttl_never_expires {
    use super::*;

    #[tokio::test]
    pub(crate) async fn in_memory_without_ttl_never_expires() {
        let clock = Arc::new(ManualClock::new(0));
        let cache = QueryFrontendCache::default().with_clock(clock.clone());
        let fixture = up_query_and_result();
        let query = fixture.query;
        let result = fixture.result;

        cache
            .insert("tenant-a", &query, result.clone())
            .await
            .unwrap();
        clock.advance(i64::from(u32::MAX));
        assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == Some(result));
    }
}

mod moving_window_reuses_cached_subranges {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct RangeBoundsCase {
        start_ms: i64,
        end_ms: i64,
    }

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
                tenant: tenant_id("tenant-a"),
                query: "up".into(),
                start_ms: 0,
                end_ms: 360_000,
                step: millis(60_000),
                admission_limits: krabka_query_frontend::AdmissionLimits::default(),
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
                tenant: tenant_id("tenant-a"),
                query: "up".into(),
                start_ms: 60_000,
                end_ms: 420_000,
                step: millis(60_000),
                admission_limits: krabka_query_frontend::AdmissionLimits::default(),
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
            .map(|query| RangeBoundsCase {
                start_ms: query.start_ms,
                end_ms: query.end_ms,
            })
            .collect::<Vec<_>>();
        assert2::assert!(
            second_starts
                == vec![
                    RangeBoundsCase {
                        start_ms: 60_000,
                        end_ms: 60_000,
                    },
                    RangeBoundsCase {
                        start_ms: 360_000,
                        end_ms: 420_000,
                    },
                ]
        );
    }
}

mod object_store_range_result_cache_expires_stale_objects {
    use super::*;

    #[tokio::test]
    pub(crate) async fn object_store_range_result_cache_expires_stale_objects() {
        let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let clock = Arc::new(ManualClock::new(5_000_000));
        let cache = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string())
            .with_ttl(secs(30))
            .with_clock(clock.clone());
        let fixture = up_query_and_result();
        let query = fixture.query;
        let result = fixture.result;

        assert_ttl_hit_then_miss(&cache, &clock, &query, &result, 29_000, 2_000).await;

        cache
            .insert("tenant-a", &query, result)
            .await
            .expect("cache insert");
        clock.advance(31_000);
        assert2::assert!(
            krabka_query_frontend::QueryCache::sweep(&cache)
                .await
                .unwrap()
                == 1
        );
    }
}

mod object_store_range_result_cache_persists_across_instances {
    use super::{cached_up_matrix::one_up_api_sample_matrix, *};

    #[tokio::test]
    pub(crate) async fn object_store_range_result_cache_persists_across_instances() {
        let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let first =
            ObjectStoreQueryFrontendCache::new(object_store.clone(), "query-cache".to_string());
        let second = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string());
        let query = up_range_query(0, Some(QueryShard { index: 1, total: 2 }));
        let result = one_up_api_sample_matrix();

        first
            .insert("tenant-a", &query, result.clone())
            .await
            .unwrap();

        assert2::assert!(second.get("tenant-a", &query).await.unwrap() == Some(result));
    }
}

mod range_result_cache_is_scoped_by_tenant_query_range_step_and_shard {
    use super::{cached_up_matrix::one_up_api_sample_matrix, *};

    #[tokio::test]
    pub(crate) async fn range_result_cache_is_scoped_by_tenant_query_range_step_and_shard() {
        let cache = QueryFrontendCache::default();
        let query = up_range_query(60_000, Some(QueryShard { index: 1, total: 2 }));
        let result = one_up_api_sample_matrix();

        cache
            .insert("tenant-a", &query, result.clone())
            .await
            .unwrap();

        assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == Some(result));
        assert2::assert!(cache.get("tenant-b", &query).await.unwrap() == None);

        let other_shard = FrontendRangeQuery {
            shard: Some(QueryShard { index: 2, total: 2 }),
            ..query
        };
        assert2::assert!(cache.get("tenant-a", &other_shard).await.unwrap() == None);
    }
}

mod range_result_cache_returns_owned_results {
    use super::*;

    #[tokio::test]
    pub(crate) async fn range_result_cache_returns_owned_results() {
        let cache = QueryFrontendCache::default();
        let query = up_range_query(0, None);
        let result = one_sample_matrix(labels(&[crate::test_support::TestLabelPair::new(
            "__name__", "up",
        )]));

        cache.insert("tenant-a", &query, result).await.unwrap();
        let Some(AnnotatedQueryResult {
            result: QueryResult::RangeMatrix(mut first_hit),
            ..
        }) = cache.get("tenant-a", &query).await.unwrap()
        else {
            panic!("cached range matrix");
        };
        first_hit[0].samples.clear();

        let Some(AnnotatedQueryResult {
            result: QueryResult::RangeMatrix(second_hit),
            ..
        }) = cache.get("tenant-a", &query).await.unwrap()
        else {
            panic!("cached range matrix");
        };
        assert2::assert!(
            second_hit[0].samples == vec![RangeSeriesSample::new(0, SampleValue::Float(1.0))]
        );
    }
}
