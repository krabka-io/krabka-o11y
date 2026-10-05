use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert2::assert;
use krabka_blockstore::{BlockStore, LabelMatcher, Labels, MatchOp, TenantId};
use krabka_metrics::{LimitError, ObjectStoreCompactionIndexSink};
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
    objects: Arc<dyn ObjectStore>,
}

fn labels() -> Labels {
    Labels::from_pairs([("__name__", "up"), ("job", "api")])
}

async fn fixture(cold: &[(&str, i64)]) -> Fixture {
    let objects = Arc::new(CountingObjectStore::new(
        Arc::new(AtomicUsize::new(0)),
        Time::ZERO,
    ));
    let reads = Arc::clone(&objects.parquet_reads);
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
        objects,
    }
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
async fn cold_newer_or_missing_series_fall_back_and_stale_hot_markers_stay_selected() {
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
                .is_none()
        );
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
