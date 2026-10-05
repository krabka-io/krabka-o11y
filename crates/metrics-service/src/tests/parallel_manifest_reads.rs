use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert2::assert;
use krabka_blockstore::{BlockStore, TenantId};
use krabka_metrics::ObjectStoreCompactionIndexSink;
use krabka_promql::{EngineOpts, MetricStore, PromqlEngine, QueryResult, WalHead};
use krabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

use super::{CountingObjectStore, write_float_manifest};
use crate::RefreshingMetricBlockStore;

#[tokio::test]
async fn manifest_reads_are_bounded_and_moving_windows_observe_publication_and_retirement() {
    let mut objects = CountingObjectStore::new(Arc::new(AtomicUsize::new(0)), Time::ZERO);
    objects.manifest_get_delay = millis(5);
    let peak = Arc::clone(&objects.manifest_peak);
    let objects: Arc<dyn ObjectStore> = Arc::new(objects);
    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&objects), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&objects));
    for n in 0..9 {
        write_float_manifest(
            &writer,
            &sink,
            "tenant-a",
            &format!("job-{n}"),
            10_000,
            &format!("metrics/tenant-a/float/{n}.parquet"),
            n,
        )
        .await;
    }
    let store = Arc::new(RefreshingMetricBlockStore::new(
        Arc::clone(&objects),
        base,
        "metrics",
        WalHead::new(),
    ));
    let engine = PromqlEngine::new(Arc::clone(&store), EngineOpts::default());
    let tenant = TenantId::new("tenant-a").unwrap();
    let value = |count, time_ms| -> QueryResult {
        serde_json::from_value(serde_json::json!({
            "InstantVector": [{"labels": {}, "ts_ms": time_ms, "value": {"Float": count}}]
        }))
        .unwrap()
    };
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 12_000)
            .await
            .unwrap()
            == value(9.0, 12_000)
    );
    assert!(peak.load(Ordering::SeqCst) > 1);
    assert!(peak.load(Ordering::SeqCst) <= 4);
    let captured = PromqlEngine::new(
        Arc::new(store.current_store(0, 12_000).await.unwrap()),
        EngineOpts::default(),
    );
    // Advancing the end still lists new manifests. Reusing unchanged index
    // snapshots must not turn the TTL into a stale read of a widened window.
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 13_000)
            .await
            .unwrap()
            == value(9.0, 13_000)
    );
    write_float_manifest(
        &writer,
        &sink,
        "tenant-a",
        "new",
        13_500,
        "metrics/tenant-a/float/new.parquet",
        9,
    )
    .await;
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 14_000)
            .await
            .unwrap()
            == value(10.0, 14_000)
    );
    objects
        .delete(&Path::from("metrics/tenant-a/float/0.parquet.index"))
        .await
        .unwrap();
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 15_000)
            .await
            .unwrap()
            == value(9.0, 15_000)
    );
    assert!(
        captured
            .query_instant(&tenant, "sum(up)", 12_000)
            .await
            .unwrap()
            == value(9.0, 12_000)
    );
    // A malformed live manifest is still a query failure, including when other
    // concurrent reads have already succeeded.
    objects
        .put(
            &Path::from("metrics/tenant-a/float/bad.index"),
            "invalid".into(),
        )
        .await
        .unwrap();
    assert!(
        engine
            .query_instant(&tenant, "sum(up)", 16_000)
            .await
            .is_err()
    );
    // The immutable snapshot is unaffected by failed publication.
    assert!(
        captured
            .query_instant(&tenant, "sum(up)", 12_000)
            .await
            .unwrap()
            == value(9.0, 12_000)
    );
    // Keep the trait's full-scan route covered as well as the instant helper.
    assert!(store.series("tenant-a", &[], 0, 16_000).await.is_err());
}
