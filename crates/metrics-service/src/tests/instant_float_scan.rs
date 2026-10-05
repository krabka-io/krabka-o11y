use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert2::assert;
use krabka_blockstore::{BlockStore, LabelMatcher, Labels, MatchOp, TenantId};
use krabka_metrics::{
    BucketSpan, LimitError, NativeHistogram, ObjectStoreCompactionIndexSink, ResetHint,
};
use krabka_promql::{
    EngineOpts, InMemoryMetricStore, MetricStore, PromqlEngine, PromqlError, QueryResult, WalHead,
};
use krabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};

use super::{CountingObjectStore, write_float_manifest};
use crate::RefreshingMetricBlockStore;

struct Fixture {
    store: Arc<RefreshingMetricBlockStore>,
    head: WalHead,
    reads: Arc<AtomicUsize>,
    list_calls: Arc<AtomicUsize>,
    objects: Arc<dyn ObjectStore>,
    parquet_keys: Arc<std::sync::Mutex<std::collections::BTreeSet<String>>>,
}

fn labels() -> Labels {
    Labels::from_pairs([("__name__", "up"), ("job", "api")])
}

async fn fixture(cold: &[(&str, i64)]) -> Fixture {
    let list_calls = Arc::new(AtomicUsize::new(0));
    let objects = Arc::new(CountingObjectStore::new(
        Arc::clone(&list_calls),
        Time::ZERO,
    ));
    let reads = Arc::clone(&objects.parquet_reads);
    let parquet_keys = Arc::clone(&objects.parquet_keys);
    let objects: Arc<dyn ObjectStore> = objects;
    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&objects), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&objects));
    for (offset, &(job, stamp)) in cold.iter().enumerate() {
        write_float_manifest(
            &writer,
            &sink,
            "tenant-a",
            job,
            stamp,
            &format!("metrics/tenant-a/float/{offset}.parquet"),
            offset as i64,
        )
        .await;
    }
    let mut hot = InMemoryMetricStore::new();
    // Out-of-order arrivals and a conflicting duplicate. The first hot row
    // at timestamp 11,000 wins, and its creation timestamp remains attached.
    for (stamp, value, start) in [
        (9_000, 3.0, None),
        (11_000, 7.0, Some(5_000)),
        (11_000, 99.0, None),
        (10_000, 4.0, None),
    ] {
        hot.push_float_with_start_timestamp("tenant-a", labels(), stamp, value, start);
    }
    let head = WalHead::from_store(hot);
    let store = Arc::new(RefreshingMetricBlockStore::new(
        Arc::clone(&objects),
        base,
        "metrics",
        head.clone(),
    ));
    Fixture {
        store,
        head,
        reads,
        list_calls,
        objects,
        parquet_keys,
    }
}

#[tokio::test]
async fn refreshing_store_preserves_shared_labels_and_tenant_deletion() {
    let fixture = fixture(&[]).await;
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let hot = fixture
        .head
        .series_shared("tenant-a", &matchers, 5_000, 12_000)
        .await
        .unwrap();
    let shared = fixture
        .store
        .series_shared("tenant-a", &matchers, 5_000, 12_000)
        .await
        .unwrap();
    assert!(shared.len() == 1);
    assert!(shared[0].as_ref() == &labels());
    assert!(Arc::ptr_eq(&shared[0], &hot[0]));
    let tenant = TenantId::new("tenant-a").unwrap();
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
    let expected: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {"__name__": "up", "job": "api"},
            "ts_ms": 12_000, "value": {"Float": 7.0}}]
    }))
    .unwrap();
    assert!(engine.query_instant(&tenant, "up", 12_000).await.unwrap() == expected);
    assert!(
        fixture
            .store
            .series_shared("other", &matchers, 5_000, 12_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .store
            .series_shared("tenant-a", &matchers, 12_001, 13_000)
            .await
            .unwrap()
            .is_empty()
    );

    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&fixture.objects), base);
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&fixture.objects));
    write_float_manifest(
        &writer,
        &sink,
        "tenant-a",
        "api",
        10_000,
        "metrics/tenant-a/float/published.parquet",
        1,
    )
    .await;
    fixture.store.invalidate().await;
    let published = fixture
        .store
        .series_shared("tenant-a", &matchers, 5_000, 12_000)
        .await
        .unwrap();
    assert!(published.len() == 1);
    assert!(published[0].as_ref() == &labels());
    assert!(!Arc::ptr_eq(&published[0], &hot[0]));
    assert!(Arc::ptr_eq(&shared[0], &hot[0]));
    let repeated = fixture
        .store
        .series_shared("tenant-a", &matchers, 5_000, 12_000)
        .await
        .unwrap();
    assert!(repeated.len() == 1 && repeated[0].as_ref() == &labels());
    assert!(Arc::ptr_eq(&repeated[0], &published[0]));
    let cold_first_scan = fixture
        .store
        .try_latest_float_scan("tenant-a", &matchers, 5_000, 5_001, 12_000, 5)
        .await
        .unwrap()
        .unwrap();
    assert!(cold_first_scan.samples == vec![(labels().fingerprint(), 11_000, 7.0, Some(5_000))]);
    assert!(cold_first_scan.labels.len() == 1);
    assert!(Arc::ptr_eq(
        &cold_first_scan.labels[&labels().fingerprint()],
        &published[0]
    ));

    fixture
        .objects
        .put(
            &Path::from("mimir-tenant-deletions/tenant-a.json"),
            PutPayload::from_static(b"{}"),
        )
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .series_shared("tenant-a", &matchers, 5_000, 12_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !fixture
            .head
            .series_shared("tenant-a", &matchers, 5_000, 12_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(shared[0].as_ref() == &labels());
    assert!(fixture.reads.load(Ordering::Relaxed) == 0);
}

#[tokio::test]
async fn dominated_cold_blocks_are_not_read_and_limits_still_count_the_full_window() {
    let fixture = fixture(&[("api", 10_000)]).await;
    let tenant = TenantId::new("tenant-a").unwrap();
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let latest = fixture
        .store
        .try_latest_float_samples("tenant-a", &matchers, 5_000, 12_000, 5)
        .await
        .unwrap();
    assert!(latest == Some(vec![(labels().fingerprint(), 11_000, 7.0, Some(5_000))]));
    let engine = PromqlEngine::new(
        Arc::clone(&fixture.store),
        EngineOpts {
            max_samples: 5,
            ..EngineOpts::default()
        },
    );
    let expected: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {"__name__": "up", "job": "api"},
            "ts_ms": 12_000, "value": {"Float": 7.0}}]
    }))
    .unwrap();
    assert!(engine.query_instant(&tenant, "up", 12_000).await.unwrap() == expected);
    let aggregate: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {}, "ts_ms": 12_000, "value": {"Float": 7.0}}]
    }))
    .unwrap();
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 12_000)
            .await
            .unwrap()
            == aggregate
    );
    assert!(fixture.reads.load(Ordering::SeqCst) == 0);

    // The generic merged store is a control that always takes the full scan.
    let control = PromqlEngine::new(
        Arc::new(fixture.store.current_store(5_000, 12_000).await.unwrap()),
        EngineOpts::default(),
    );
    for query in [
        "up",
        "sum(up)",
        "avg(up)",
        "timestamp(up)",
        "up offset 2s",
        "max_over_time(up[20s])",
    ] {
        assert!(
            engine.query_instant(&tenant, query, 12_000).await.unwrap()
                == control.query_instant(&tenant, query, 12_000).await.unwrap()
        );
    }
    assert!(fixture.reads.load(Ordering::SeqCst) > 0);
    assert!(
        engine
            .query_range(&tenant, "up", 9_000, 12_000, secs(1))
            .await
            .unwrap()
            == control
                .query_range(&tenant, "up", 9_000, 12_000, secs(1))
                .await
                .unwrap()
    );

    // The raw upper bound includes overlapping cold rows and duplicate hot
    // rows. When it is too high, fall back to exact deduplication, not rejection.
    assert!(
        fixture
            .store
            .try_latest_float_samples("tenant-a", &matchers, 5_000, 12_000, 3)
            .await
            .unwrap()
            .is_none()
    );
    let limited = |max_samples| {
        PromqlEngine::new(
            Arc::clone(&fixture.store),
            EngineOpts {
                max_samples,
                ..EngineOpts::default()
            },
        )
    };
    assert!(
        limited(3)
            .query_instant(&tenant, "up", 12_000)
            .await
            .unwrap()
            == expected
    );
    assert!(matches!(
        limited(2).query_instant(&tenant, "up", 12_000).await,
        Err(PromqlError::Limit(LimitError::SamplesPerQueryExceeded {
            limit: 2,
            ..
        }))
    ));
}

#[tokio::test]
async fn cold_newer_or_missing_series_are_read_and_stale_hot_markers_stay_selected() {
    let tenant = TenantId::new("tenant-a").unwrap();
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    for cold in [vec![("api", 12_000)], vec![("api", 10_000), ("db", 10_000)]] {
        let fixture = fixture(&cold).await;
        assert!(
            fixture
                .store
                .try_latest_float_samples("tenant-a", &matchers, 5_000, 14_000, 100)
                .await
                .unwrap()
                .is_some()
        );
        let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
        let control = PromqlEngine::new(
            Arc::new(fixture.store.current_store(5_000, 14_000).await.unwrap()),
            EngineOpts::default(),
        );
        for query in [
            "up",
            "sum(up)",
            "avg(up)",
            "up offset 3s",
            "up @ 11",
            "up @ 12",
        ] {
            assert!(
                engine.query_instant(&tenant, query, 14_000).await.unwrap()
                    == control.query_instant(&tenant, query, 14_000).await.unwrap()
            );
        }
    }
    let fixture = fixture(&[("api", 10_000)]).await;
    fixture.head.update(|hot| {
        hot.push_float(
            "tenant-a",
            labels(),
            13_000,
            f64::from_bits(0x7ff0_0000_0000_0002),
        )
    });
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
    assert!(
        engine.query_instant(&tenant, "up", 14_000).await.unwrap()
            == QueryResult::InstantVector(Vec::new())
    );
    assert!(fixture.reads.load(Ordering::SeqCst) == 0);
    let limited = PromqlEngine::new(
        Arc::clone(&fixture.store),
        EngineOpts {
            max_fetched_series: 1,
            ..EngineOpts::default()
        },
    );
    fixture.head.update(|hot| {
        hot.push_float(
            "tenant-a",
            Labels::from_pairs([("__name__", "up"), ("job", "db")]),
            13_000,
            9.0,
        )
    });
    assert!(matches!(
        limited.query_instant(&tenant, "up", 14_000).await,
        Err(PromqlError::Limit(
            LimitError::SeriesPerQueryExceeded { .. }
        ))
    ));
    fixture
        .objects
        .put(
            &Path::from("mimir-tenant-deletions/tenant-a.json"),
            PutPayload::from_static(b"{}"),
        )
        .await
        .unwrap();
    assert!(
        engine.query_instant(&tenant, "up", 14_000).await.unwrap()
            == QueryResult::InstantVector(Vec::new())
    );
}

#[tokio::test]
async fn latest_aggregate_preserves_compensated_sums_across_series() {
    let fixture = fixture(&[("api", 10_000)]).await;
    let tenant = TenantId::new("tenant-a").unwrap();
    fixture.head.update(|hot| {
        for (job, value) in [("api", 1e16), ("db", 1.0), ("worker", -1e16)] {
            hot.push_float(
                "tenant-a",
                Labels::from_pairs([("__name__", "up"), ("job", job)]),
                13_000,
                value,
            );
        }
    });
    let captured = fixture
        .store
        .try_latest_float_scan(
            "tenant-a",
            &[LabelMatcher::new("__name__", MatchOp::Eq, "up")],
            5_000,
            5_001,
            14_000,
            8,
        )
        .await
        .unwrap()
        .unwrap();
    // Fixed canonical fingerprints keep the ledger independent of the scan's
    // grouping and traversal. Arrival order differs from fingerprint order.
    assert!(
        captured.samples
            == vec![
                (0x01b8_550f_8631_0fb1, 13_000, -1e16, None),
                (0x54b3_866d_77d1_1fda, 13_000, 1e16, None),
                (0x7115_4d29_6d09_a369, 13_000, 1.0, None),
            ]
    );
    assert!(
        captured
            .labels
            .into_iter()
            .map(|(fp, labels)| (fp, labels.as_ref().clone()))
            .collect::<Vec<_>>()
            == [
                (0x01b8_550f_8631_0fb1, "worker"),
                (0x54b3_866d_77d1_1fda, "api"),
                (0x7115_4d29_6d09_a369, "db"),
            ]
            .map(|(fp, job)| (fp, Labels::from_pairs([("__name__", "up"), ("job", job)])))
            .to_vec()
    );
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
    let expected: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {}, "ts_ms": 14_000, "value": {"Float": 1.0}}]
    }))
    .unwrap();
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 14_000)
            .await
            .unwrap()
            == expected
    );
    assert!(fixture.reads.load(Ordering::SeqCst) == 0);
    let control = PromqlEngine::new(
        Arc::new(fixture.store.current_store(5_000, 14_000).await.unwrap()),
        EngineOpts::default(),
    );
    for query in ["sum(up)", "avg(up)", "sum by (job) (up)", "topk(1, up)"] {
        assert!(
            engine.query_instant(&tenant, query, 14_000).await.unwrap()
                == control.query_instant(&tenant, query, 14_000).await.unwrap()
        );
    }
}

#[tokio::test]
async fn newer_cold_block_does_not_force_reads_of_dominated_history() {
    let mut blocks = vec![("api", 10_000); 9];
    blocks.push(("api", 12_000));
    let fixture = fixture(&blocks).await;
    let tenant = TenantId::new("tenant-a").unwrap();
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
    let expected: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {}, "ts_ms": 14_000, "value": {"Float": 1.0}}]
    }))
    .unwrap();
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 14_000)
            .await
            .unwrap()
            == expected
    );
    assert!(
        fixture
            .parquet_keys
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            == vec!["metrics/tenant-a/float/9.parquet".to_string()]
    );
    let control = PromqlEngine::new(
        Arc::new(fixture.store.current_store(5_000, 14_000).await.unwrap()),
        EngineOpts::default(),
    );
    assert!(
        control
            .query_instant(&tenant, "sum(up)", 14_000)
            .await
            .unwrap()
            == expected
    );
    assert!(fixture.parquet_keys.lock().unwrap().len() == 10);
    // A retirement race must retain the ordinary path's warning and hot value.
    fixture
        .objects
        .delete(&Path::from("metrics/tenant-a/float/9.parquet"))
        .await
        .unwrap();
    let candidate = engine
        .query_instant_with_annotations(&tenant, "sum(up)", 15_000)
        .await
        .unwrap();
    let reference = control
        .query_instant_with_annotations(&tenant, "sum(up)", 15_000)
        .await
        .unwrap();
    assert!(candidate == reference);
}

#[tokio::test]
async fn conflicting_cold_ties_keep_the_full_scan() {
    let fixture = fixture(&[("api", 12_000)]).await;
    let writer = BlockStore::new(
        Arc::clone(&fixture.objects),
        url::Url::parse("memory:///").unwrap(),
    );
    let batch =
        krabka_metrics::encode_float_samples(&[(labels().fingerprint(), 12_000, 2.0, None)])
            .unwrap();
    let block = writer
        .writer()
        .write_block(
            "tenant-a",
            "metrics/tenant-a/float/tie.parquet",
            krabka_metrics::float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let plan = krabka_metrics::CompactionObjectPlan {
        block_key: block.object_key.clone(),
        index_key: "metrics/tenant-a/float/tie.parquet.index".into(),
        first_offset: 1,
        last_offset: 1,
        row_count: block.row_count,
    };
    let manifest = krabka_metrics::CompactionIndexManifest::from_block_meta(
        krabka_metrics::MetricBlockKind::Float,
        &plan,
        &block,
        vec![krabka_metrics::CompactionSeriesLabels {
            fingerprint: labels().fingerprint(),
            labels: labels(),
        }],
    );
    krabka_metrics::CompactionIndexSink::write_manifest(
        &ObjectStoreCompactionIndexSink::new(Arc::clone(&fixture.objects)),
        &manifest,
    )
    .await
    .unwrap();
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    assert!(
        fixture
            .store
            .try_latest_float_samples("tenant-a", &matchers, 5_000, 14_000, 100)
            .await
            .unwrap()
            .is_none()
    );
    let tenant = TenantId::new("tenant-a").unwrap();
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
    let control = PromqlEngine::new(
        Arc::new(fixture.store.current_store(5_000, 14_000).await.unwrap()),
        EngineOpts::default(),
    );
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 14_000)
            .await
            .unwrap()
            == control
                .query_instant(&tenant, "sum(up)", 14_000)
                .await
                .unwrap()
    );
}

#[tokio::test]
async fn latest_aggregate_refreshes_one_window_for_samples_and_labels() {
    let fixture = fixture(&[("api", 10_000)]).await;
    let tenant = TenantId::new("tenant-a").unwrap();
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
    let expected: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {}, "ts_ms": 12_000, "value": {"Float": 7.0}}]
    }))
    .unwrap();
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 12_000)
            .await
            .unwrap()
            == expected
    );
    // The first request starts with no cached manifest window. Separate sample
    // and label operations would list twice for their different lower bounds.
    assert!(fixture.list_calls.load(Ordering::SeqCst) == 1);
    assert!(fixture.reads.load(Ordering::SeqCst) == 0);
}

#[tokio::test]
async fn latest_scan_keeps_boundary_labels_limits_and_captured_precedence() {
    let fixture = fixture(&[("api", 10_000), ("cold-boundary", 9_000)]).await;
    let tenant = TenantId::new("tenant-a").unwrap();
    let job_labels = |job| Labels::from_pairs([("__name__", "up"), ("job", job)]);
    fixture.head.update(|hot| {
        for (job, stamp) in [
            ("float-boundary", 9_000),
            ("outside", 8_000),
            ("future", 13_000),
        ] {
            hot.push_float("tenant-a", job_labels(job), stamp, 42.0);
        }
        hot.push_float("tenant-b", labels(), 11_000, 42.0);
        hot.push_histogram(
            "tenant-a",
            job_labels("hist-boundary"),
            9_000,
            NativeHistogram {
                schema: 0,
                is_float: false,
                reset_hint: ResetHint::No,
                zero_threshold: 1e-128,
                zero_count: 0.0,
                count: 2.0,
                sum: 3.0,
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 1,
                }],
                positive_counts: vec![2.0],
                negative_spans: Vec::new(),
                negative_counts: Vec::new(),
                custom_values: None,
                start_timestamp_ms: None,
            },
        );
    });
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let captured = fixture
        .store
        .try_latest_float_scan("tenant-a", &matchers, 9_000, 9_001, 12_000, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(captured.samples == vec![(labels().fingerprint(), 11_000, 7.0, Some(5_000))]);
    let mut expected_labels = ["api", "cold-boundary", "float-boundary", "hist-boundary"]
        .map(job_labels)
        .to_vec();
    expected_labels.sort_by_key(Labels::fingerprint);
    assert!(
        captured
            .labels
            .values()
            .map(|labels| labels.as_ref())
            .collect::<Vec<_>>()
            == expected_labels.iter().collect::<Vec<_>>()
    );
    let hot_labels = fixture
        .head
        .series_shared("tenant-a", &matchers, 9_000, 12_000)
        .await
        .unwrap();
    for hot in &hot_labels {
        let resolved = &captured.labels[&hot.fingerprint()];
        assert!(Arc::ptr_eq(resolved, hot) == (hot.as_ref() != &labels()));
    }
    assert!(fixture.reads.load(Ordering::SeqCst) == 0);
    assert!(
        fixture
            .store
            .try_latest_float_scan("tenant-a", &matchers, 9_000, 9_001, 12_000, 3)
            .await
            .unwrap()
            .is_none()
    );

    let opts = EngineOpts {
        lookback_delta: secs(3),
        ..EngineOpts::default()
    };
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), opts.clone());
    let expected: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {"__name__": "up", "job": "api"},
            "ts_ms": 12_000, "value": {"Float": 7.0}}]
    }))
    .unwrap();
    assert!(engine.query_instant(&tenant, "up", 12_000).await.unwrap() == expected);
    let control = PromqlEngine::new(
        Arc::new(fixture.store.current_store(5_000, 12_000).await.unwrap()),
        opts.clone(),
    );
    for query in [
        "up",
        "sum(up)",
        "avg(up)",
        "timestamp(up)",
        "up offset 2s",
        "up @ 10",
    ] {
        assert!(
            engine.query_instant(&tenant, query, 12_000).await.unwrap()
                == control.query_instant(&tenant, query, 12_000).await.unwrap()
        );
    }
    for (max_samples, max_fetched_series) in [(3, 4), (4, 3), (1, 4)] {
        let limited = PromqlEngine::new(
            Arc::clone(&fixture.store),
            EngineOpts {
                max_samples,
                max_fetched_series,
                ..opts.clone()
            },
        );
        let result = limited.query_instant(&tenant, "up", 12_000).await;
        match (max_samples, max_fetched_series) {
            (3, 4) => assert!(result.unwrap() == expected),
            (4, 3) => assert!(matches!(
                result,
                Err(PromqlError::Limit(
                    LimitError::SeriesPerQueryExceeded { .. }
                ))
            )),
            (1, 4) => assert!(matches!(
                result,
                Err(PromqlError::Limit(LimitError::SamplesPerQueryExceeded {
                    limit: 1,
                    ..
                }))
            )),
            _ => unreachable!(),
        }
    }
    fixture
        .objects
        .put(
            &Path::from("mimir-tenant-deletions/tenant-a.json"),
            PutPayload::from_static(b"{}"),
        )
        .await
        .unwrap();
    assert!(
        engine.query_instant(&tenant, "up", 12_000).await.unwrap()
            == QueryResult::InstantVector(Vec::new())
    );
    fixture.head.delete_tenant("tenant-a");
    assert!(captured.samples == vec![(labels().fingerprint(), 11_000, 7.0, Some(5_000))]);
    assert!(
        captured
            .labels
            .values()
            .map(|labels| labels.as_ref())
            .collect::<Vec<_>>()
            == expected_labels.iter().collect::<Vec<_>>()
    );
}
