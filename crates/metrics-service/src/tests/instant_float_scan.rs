use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use assert2::assert;
use krabka_blockstore::{BlockStore, MatchOp, TenantId};
use krabka_metrics::{
    BucketSpan, LimitError, NativeHistogram, ObjectStoreCompactionIndexSink, ResetHint,
};
use krabka_promql::{
    Annotations, EngineOpts, InMemoryMetricStore, MetricStore, PromqlEngine, PromqlError,
    PromqlLabels as Labels, PromqlMatcher as LabelMatcher, QueryResult, SampleValue, WalHead,
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
            i64::try_from(offset).unwrap(),
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
    let cold_first_scan = fixture
        .store
        .try_latest_float_scan("tenant-a", &matchers, 5_000, 5_001, 12_000, 5)
        .await
        .unwrap()
        .unwrap();
    assert!(cold_first_scan.samples == vec![(labels().fingerprint(), 11_000, 7.0, Some(5_000))]);
    assert!(cold_first_scan.labels.len() == 1);
    assert!(cold_first_scan.labels[&labels().fingerprint()].as_ref() == &labels());
    assert!(!Arc::ptr_eq(
        &cold_first_scan.labels[&labels().fingerprint()],
        &hot[0]
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
        );
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
        );
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
            .map(AsRef::as_ref)
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
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), opts);
    let expected: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {"__name__": "up", "job": "api"},
            "ts_ms": 12_000, "value": {"Float": 7.0}}]
    }))
    .unwrap();
    assert!(engine.query_instant(&tenant, "up", 12_000).await.unwrap() == expected);
    let control = PromqlEngine::new(
        Arc::new(fixture.store.current_store(5_000, 12_000).await.unwrap()),
        opts,
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
                ..opts
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
            .map(AsRef::as_ref)
            .collect::<Vec<_>>()
            == expected_labels.iter().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn latest_scan_keeps_retained_history_boundary_labels_and_historical_values() {
    let fixture = fixture(&[]).await;
    let boundary = Labels::from_pairs([("__name__", "up"), ("job", "float-boundary")]);
    let histogram = Labels::from_pairs([("__name__", "up"), ("job", "hist-boundary")]);
    fixture.head.update(|hot| {
        hot.push_float("tenant-a", labels(), 1_000, 1.0);
        hot.push_float("tenant-a", boundary.clone(), 8_000, 40.0);
        hot.push_float("tenant-a", boundary.clone(), 9_000, 42.0);
        hot.push_float(
            "tenant-a",
            Labels::from_pairs([("__name__", "down"), ("job", "future")]),
            13_000,
            99.0,
        );
        hot.push_histogram(
            "tenant-a",
            histogram.clone(),
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
        .try_latest_float_scan("tenant-a", &matchers, 9_000, 9_001, 12_000, 5)
        .await
        .unwrap()
        .unwrap();
    assert!(captured.samples == vec![(labels().fingerprint(), 11_000, 7.0, Some(5_000))]);
    assert!(
        captured
            .labels
            .iter()
            .map(|(fp, labels)| (*fp, labels.as_ref().clone()))
            .collect::<BTreeMap<_, _>>()
            == [labels(), boundary, histogram]
                .into_iter()
                .map(|labels| (labels.fingerprint(), labels))
                .collect::<BTreeMap<_, _>>()
    );

    // Retained old rows and duplicate timestamps can exceed the summary's
    // upper bound while the ordinary in-window scan still fits the limit.
    let tenant = TenantId::new("tenant-a").unwrap();
    let opts = EngineOpts {
        lookback_delta: secs(3),
        max_samples: 3,
        ..EngineOpts::default()
    };
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), opts);
    let expected: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {"__name__": "up", "job": "api"},
            "ts_ms": 12_000, "value": {"Float": 7.0}}]
    }))
    .unwrap();
    assert!(engine.query_instant(&tenant, "up", 12_000).await.unwrap() == expected);
    // Aggregates' full scan counts the inclusive label boundary. Preserve
    // the original latest hot-row path when a summary bound exceeds the cap.
    let aggregate: QueryResult = serde_json::from_value(serde_json::json!({
        "InstantVector": [{"labels": {}, "ts_ms": 12_000,
            "value": {"Float": 7.0}}]
    }))
    .unwrap();
    for query in ["sum(up)", "avg(up)"] {
        assert!(engine.query_instant(&tenant, query, 12_000).await.unwrap() == aggregate);
    }
    let limited = PromqlEngine::new(
        Arc::clone(&fixture.store),
        EngineOpts {
            max_fetched_series: 2,
            ..opts
        },
    );
    for query in ["up", "sum(up)", "avg(up)"] {
        assert!(matches!(
            limited.query_instant(&tenant, query, 12_000).await,
            Err(PromqlError::Limit(
                LimitError::SeriesPerQueryExceeded { .. }
            ))
        ));
    }
    let capped = PromqlEngine::new(
        Arc::clone(&fixture.store),
        EngineOpts {
            max_samples: 1,
            ..opts
        },
    );
    for query in ["sum(up)", "avg(up)"] {
        assert!(matches!(
            capped.query_instant(&tenant, query, 12_000).await,
            Err(PromqlError::Limit(LimitError::SamplesPerQueryExceeded {
                limit: 1,
                ..
            }))
        ));
    }

    // A matching future row cannot cause the historical in-window value to
    // disappear. The earlier unmatched future series does not affect it.
    fixture
        .head
        .update(|hot| hot.push_float("tenant-a", labels(), 13_000, 99.0));
    assert!(engine.query_instant(&tenant, "up", 12_000).await.unwrap() == expected);
    let historical = fixture
        .store
        .try_latest_float_scan("tenant-a", &matchers, 9_000, 9_001, 12_000, 5)
        .await
        .unwrap()
        .unwrap();
    assert!(historical.samples == captured.samples && historical.labels == captured.labels);
    assert!(fixture.reads.load(Ordering::SeqCst) == 0);

    // The combined hot/cold bound can exceed the cap even when the original
    // hot-row count plus the selected cold block count still fits exactly.
    let cold_fixture = self::fixture(&[("api", 10_000)]).await;
    cold_fixture
        .head
        .update(|hot| hot.push_float("tenant-a", labels(), 1_000, 1.0));
    let exact = cold_fixture
        .store
        .try_latest_float_scan("tenant-a", &matchers, 9_000, 9_001, 12_000, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(exact.samples == vec![(labels().fingerprint(), 11_000, 7.0, Some(5_000))]);
    assert!(
        exact
            .labels
            .iter()
            .map(|(fp, labels)| (*fp, labels.as_ref().clone()))
            .collect::<BTreeMap<_, _>>()
            == BTreeMap::from([(labels().fingerprint(), labels())])
    );
    assert!(cold_fixture.reads.load(Ordering::SeqCst) == 0);
}

/// Keep the entire public response and annotations, including nonfinite float bits.
fn float_response_ledger(
    (result, annotations): (QueryResult, Annotations),
) -> (serde_json::Value, Vec<u64>, Annotations) {
    let QueryResult::InstantVector(samples) = &result else {
        panic!("expected an instant vector");
    };
    let bits = samples
        .iter()
        .map(|sample| {
            let SampleValue::Float(value) = &sample.value else {
                panic!("expected a float sample");
            };
            value.to_bits()
        })
        .collect();
    (serde_json::to_value(result).unwrap(), bits, annotations)
}

#[tokio::test]
async fn global_latest_aggregates_preserve_cold_hot_query_ledgers() {
    let fixture = fixture(&[("api", 10_000), ("db", 10_000), ("boundary", 9_000)]).await;
    // Independent effective-row ledger: the first hot 11,000 row wins its
    // conflicting duplicate, and hot api wins the equal-time cold row.
    let mut reference = InMemoryMetricStore::new();
    for (stamp, value, start) in [
        (9_000, 3.0, None),
        (11_000, 7.0, Some(5_000)),
        (10_000, 4.0, None),
    ] {
        reference.push_float_with_start_timestamp("tenant-a", labels(), stamp, value, start);
    }
    for (job, stamp) in [("db", 10_000), ("boundary", 9_000)] {
        reference.push_float(
            "tenant-a",
            Labels::from_pairs([("__name__", "up"), ("job", job)]),
            stamp,
            1.0,
        );
    }
    let tenant = TenantId::new("tenant-a").unwrap();
    let opts = EngineOpts {
        lookback_delta: secs(3),
        ..EngineOpts::default()
    };
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), opts);
    let control = PromqlEngine::new(Arc::new(reference), opts);
    for (query, value) in [
        ("sum(up)", 8.0_f64),
        ("avg(up)", 4.0),
        ("sum by () (up)", 8.0),
        ("avg by () (up)", 4.0),
        ("sum(up @ 10)", 6.0),
        ("avg(up @ 10)", 2.0),
        ("sum(up offset 2s)", 6.0),
        ("avg(up offset 2s)", 2.0),
        ("sum(up @ start())", 8.0),
        ("avg(up @ end())", 4.0),
    ] {
        let actual = engine
            .query_instant_with_annotations(&tenant, query, 12_000)
            .await
            .unwrap();
        let expected = (
            serde_json::json!({"InstantVector": [{"labels": {}, "ts_ms": 12_000, "value": {"Float": value}}]}),
            vec![value.to_bits()],
            Annotations::new(),
        );
        assert!(float_response_ledger(actual) == expected);
        assert!(
            float_response_ledger(
                control
                    .query_instant_with_annotations(&tenant, query, 12_000)
                    .await
                    .unwrap()
            ) == expected
        );
    }
    // These expressions retain their established grouping and recursive plans.
    for query in [
        "sum by (job) (up)",
        "avg without (job) (up)",
        "sum((up))",
        "sum(rate(up[3s]))",
        "sum(up{job=~\"api|db\"})",
    ] {
        assert!(
            engine
                .query_instant_with_annotations(&tenant, query, 12_000)
                .await
                .unwrap()
                == control
                    .query_instant_with_annotations(&tenant, query, 12_000)
                    .await
                    .unwrap()
        );
    }
    // A range request must retain its grid, boundary rules and disabled latest scope.
    for query in ["sum(up)", "avg(up)", "sum(up @ start())", "avg(up @ end())"] {
        assert!(
            engine
                .query_range(&tenant, query, 11_000, 12_000, secs(1))
                .await
                .unwrap()
                == control
                    .query_range(&tenant, query, 11_000, 12_000, secs(1))
                    .await
                    .unwrap()
        );
    }
}

#[tokio::test]
async fn global_latest_aggregates_keep_float_bits_staleness_and_empty_groups() {
    let fixture = fixture(&[]).await;
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    let genuine_nan = f64::from_bits(0x7ff8_0000_0000_0055);
    let cases = [
        (
            "cancellation",
            &[1e16, 1.0, -1e16] as &[f64],
            Some((1.0, 1.0 / 3.0)),
        ),
        (
            "overflow",
            &[f64::MAX, f64::MAX],
            Some((f64::INFINITY, f64::MAX)),
        ),
        ("negative_zero", &[-0.0], Some((0.0, 0.0))),
        (
            "infinity",
            &[f64::INFINITY],
            Some((f64::INFINITY, f64::INFINITY)),
        ),
        (
            "opposite_infinities",
            &[f64::INFINITY, f64::NEG_INFINITY],
            None,
        ),
        ("genuine_nan", &[genuine_nan], None),
        ("all_stale", &[stale], None),
        ("empty", &[], None),
    ];
    let mut reference = InMemoryMetricStore::new();
    for (metric, values, _) in cases {
        for (index, &value) in values.iter().enumerate() {
            let series = Labels::from_pairs([("__name__", metric), ("job", &index.to_string())]);
            let start = (index % 2 == 0).then_some(0);
            reference.push_float_with_start_timestamp(
                "tenant-a",
                series.clone(),
                11_000,
                value,
                start,
            );
            // The real head includes a conflicting duplicate; the independent
            // effective-row oracle contains only the stated first winner.
            fixture.head.update(|hot| {
                hot.push_float_with_start_timestamp(
                    "tenant-a",
                    series.clone(),
                    11_000,
                    value,
                    start,
                );
                hot.push_float("tenant-a", series, 11_000, 42.0);
            });
        }
    }
    let tenant = TenantId::new("tenant-a").unwrap();
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
    let control = PromqlEngine::new(Arc::new(reference), EngineOpts::default());
    for (metric, values, expected) in cases {
        let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, metric)];
        assert!(
            fixture
                .store
                .try_latest_float_scan("tenant-a", &matchers, 0, 1, 12_000, 100)
                .await
                .unwrap()
                .is_some()
        );
        for (op, known_value) in [
            ("sum", expected.map(|pair| pair.0)),
            ("avg", expected.map(|pair| pair.1)),
        ] {
            let query = format!("{op}({metric})");
            let actual = float_response_ledger(
                engine
                    .query_instant_with_annotations(&tenant, &query, 12_000)
                    .await
                    .unwrap(),
            );
            // No fixture has __absent__: this grouping has the same result but
            // retains the original labeled-selector/shared-kernel aggregate path.
            let original_path = format!("{op} by (__absent__) ({metric})");
            assert!(
                actual
                    == float_response_ledger(
                        engine
                            .query_instant_with_annotations(&tenant, &original_path, 12_000)
                            .await
                            .unwrap()
                    )
            );
            assert!(
                actual
                    == float_response_ledger(
                        control
                            .query_instant_with_annotations(&tenant, &query, 12_000)
                            .await
                            .unwrap()
                    )
            );
            if let Some(value) = known_value {
                assert!(
                    actual
                        == (
                            serde_json::json!({"InstantVector": [{"labels": {}, "ts_ms": 12_000, "value": {"Float": value}}]}),
                            vec![value.to_bits()],
                            Annotations::new()
                        )
                );
            } else if values.is_empty() || metric == "all_stale" {
                assert!(
                    actual
                        == (
                            serde_json::json!({"InstantVector": []}),
                            Vec::new(),
                            Annotations::new()
                        )
                );
            } else {
                assert!(
                    actual.1.len() == 1
                        && f64::from_bits(actual.1[0]).is_nan()
                        && actual.1[0] != stale.to_bits()
                );
            }
        }
    }
}

#[tokio::test]
async fn global_latest_aggregates_preserve_histogram_fallback_and_annotations() {
    let fixture = fixture(&[]).await;
    let histogram = NativeHistogram {
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
        start_timestamp_ms: Some(0),
    };
    let mut reference = InMemoryMetricStore::new();
    for metric in ["histogram_only", "mixed_samples"] {
        let series = Labels::from_pairs([("__name__", metric), ("job", "histogram")]);
        reference.push_histogram("tenant-a", series.clone(), 11_000, histogram.clone());
        fixture
            .head
            .update(|hot| hot.push_histogram("tenant-a", series, 11_000, histogram.clone()));
    }
    let series = Labels::from_pairs([("__name__", "mixed_samples"), ("job", "float")]);
    reference.push_float("tenant-a", series.clone(), 11_000, 7.0);
    fixture
        .head
        .update(|hot| hot.push_float("tenant-a", series, 11_000, 7.0));
    let tenant = TenantId::new("tenant-a").unwrap();
    let engine = PromqlEngine::new(Arc::clone(&fixture.store), EngineOpts::default());
    let control = PromqlEngine::new(Arc::new(reference), EngineOpts::default());
    for metric in ["histogram_only", "mixed_samples"] {
        let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, metric)];
        assert!(
            fixture
                .store
                .try_latest_float_scan("tenant-a", &matchers, 0, 1, 12_000, 100)
                .await
                .unwrap()
                .is_none()
        );
        for op in ["sum", "avg"] {
            let query = format!("{op}({metric})");
            let actual = engine
                .query_instant_with_annotations(&tenant, &query, 12_000)
                .await
                .unwrap();
            assert!(
                actual
                    == control
                        .query_instant_with_annotations(&tenant, &query, 12_000)
                        .await
                        .unwrap()
            );
            if metric == "mixed_samples" {
                assert!(actual.0 == QueryResult::InstantVector(Vec::new()));
                assert!(actual.1.warnings.len() == 1 && actual.1.infos.is_empty());
            }
        }
    }
}
