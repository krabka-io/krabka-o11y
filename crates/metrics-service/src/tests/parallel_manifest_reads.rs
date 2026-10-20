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
        Arc::new(store.current_store("tenant-a", 0, 12_000).await.unwrap()),
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
    write_float_manifest(
        &writer,
        &sink,
        "tenant-a",
        "bad",
        15_500,
        "metrics/tenant-a/float/bad.parquet",
        10,
    )
    .await;
    objects
        .put(
            &Path::from("metrics/tenant-a/float/bad.parquet.index"),
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

#[tokio::test]
async fn tenant_and_time_catalog_work_is_constant_as_unrelated_history_grows() {
    use krabka_metrics::{CompactionIndexSink, manifest_catalog::metric_manifest_catalog_prefixes};

    let counted = Arc::new(CountingObjectStore::new(Arc::default(), Time::ZERO));
    let objects: Arc<dyn ObjectStore> = counted.clone();
    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&objects), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&objects));
    write_float_manifest(
        &writer,
        &sink,
        "tenant-a",
        "target",
        10_000,
        "metrics/tenant-a/float/target.parquet",
        0,
    )
    .await;
    let template = sink
        .read_manifest("metrics/tenant-a/float/target.parquet.index")
        .await
        .unwrap();
    let service =
        RefreshingMetricBlockStore::new(Arc::clone(&objects), base, "metrics", WalHead::new())
            .with_cold_cache_ttl(Time::ZERO);
    let mut baseline = None;
    for count in [0, 100, 2000] {
        for number in 0..count {
            for tenant in ["tenant-a".to_owned(), format!("other-{number}")] {
                let mut manifest = template.clone();
                manifest.tenant.clone_from(&tenant);
                manifest.index_key = format!("metrics/{tenant}/float/noise-{number}.index");
                manifest.block_key = format!("metrics/{tenant}/float/noise-{number}.parquet");
                if tenant == "tenant-a" {
                    manifest.min_ts = -86_400_000;
                    manifest.max_ts = -86_400_000;
                }
                sink.write_manifest(&manifest).await.unwrap();
                // Irrelevant corrupt label payloads must never be read.
                objects
                    .put(&Path::from(manifest.index_key), "invalid".into())
                    .await
                    .unwrap();
            }
        }
        counted.list_calls.store(0, Ordering::SeqCst);
        counted.listed_objects.store(0, Ordering::SeqCst);
        counted.get_calls.store(0, Ordering::SeqCst);
        counted.listed_prefixes.lock().unwrap().clear();
        let result = service.series("tenant-a", &[], 0, 20_000).await.unwrap();
        assert!(result.len() == 1);
        assert!(result[0].get("job").unwrap() == "target");
        let work = (
            counted.list_calls.load(Ordering::SeqCst),
            counted.listed_objects.load(Ordering::SeqCst),
            counted.get_calls.load(Ordering::SeqCst),
        );
        assert!(work.0 == metric_manifest_catalog_prefixes("metrics", "tenant-a", 0, 20_000).len());
        if let Some(expected) = baseline {
            assert!(work == expected);
        } else {
            baseline = Some(work);
        }
        assert!(
            counted
                .listed_prefixes
                .lock()
                .unwrap()
                .iter()
                .all(|prefix| prefix.starts_with("metrics/tenant-a/_catalog/v1/"))
        );
    }
}

#[tokio::test]
async fn tenant_snapshots_do_not_evict_each_other_and_escape_path_segments() {
    let mut counted = CountingObjectStore::new(Arc::default(), Time::ZERO);
    counted.manifest_get_delay = millis(5);
    let peak = Arc::clone(&counted.manifest_peak);
    let counted = Arc::new(counted);
    let objects: Arc<dyn ObjectStore> = counted.clone();
    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&objects), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&objects));
    for tenant in ["a/b%", "a", "a/b%-other"] {
        let segment = krabka_blockstore::escape_object_path_segment(tenant);
        write_float_manifest(
            &writer,
            &sink,
            tenant,
            tenant,
            10_000,
            &format!("metrics/{segment}/float/target.parquet"),
            0,
        )
        .await;
    }
    let service =
        RefreshingMetricBlockStore::new(Arc::clone(&objects), base, "metrics", WalHead::new());
    let results = tokio::join!(
        service.series("a/b%", &[], 0, 20_000),
        service.series("a", &[], 0, 20_000),
        service.series("a/b%-other", &[], 0, 20_000),
    );
    assert!(peak.load(Ordering::SeqCst) > 1);
    for (tenant, result) in ["a/b%", "a", "a/b%-other"]
        .into_iter()
        .zip([results.0, results.1, results.2])
    {
        let result = result.unwrap();
        assert!(result.len() == 1);
        assert!(result[0].get("job").unwrap() == tenant);
    }
    let lists = counted.list_calls.load(Ordering::SeqCst);
    for tenant in ["a/b%", "a", "a/b%-other"] {
        assert!(service.series(tenant, &[], 0, 20_000).await.unwrap().len() == 1);
    }
    assert!(counted.list_calls.load(Ordering::SeqCst) == lists);
    service.invalidate("a").await;
    for tenant in ["a/b%", "a/b%-other"] {
        assert!(service.series(tenant, &[], 0, 20_000).await.unwrap().len() == 1);
    }
    assert!(counted.list_calls.load(Ordering::SeqCst) == lists);
    assert!(service.series("a", &[], 0, 20_000).await.unwrap().len() == 1);
    assert!(
        counted.list_calls.load(Ordering::SeqCst)
            == lists
                + krabka_metrics::manifest_catalog::metric_manifest_catalog_prefixes(
                    "metrics", "a", 0, 20_000
                )
                .len()
    );
}

#[tokio::test]
async fn an_import_marker_expands_partial_catalog_listings_and_retired_references_stay_retired() {
    use krabka_metrics::manifest_catalog::{
        MetricManifestCatalog, MetricManifestReference, metric_manifest_catalog_key,
    };

    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&objects), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&objects));
    let directory = "metrics/tenant-a/uploaded/01ARZ3NDEKTSV4RRFFQ69G5FAV-0123456789abcdef";
    let mut manifests = Vec::new();
    for number in 0..2 {
        let key = format!("{directory}/{number}.parquet");
        write_float_manifest(
            &writer,
            &sink,
            "tenant-a",
            &format!("job-{number}"),
            10_000,
            &key,
            number,
        )
        .await;
        manifests.push(sink.read_manifest(&format!("{key}.index")).await.unwrap());
    }
    let service =
        RefreshingMetricBlockStore::new(Arc::clone(&objects), base, "metrics", WalHead::new())
            .with_cold_cache_ttl(Time::ZERO);
    assert!(
        service
            .series("tenant-a", &[], 0, 20_000)
            .await
            .unwrap()
            .is_empty()
    );
    // Model a LIST that saw only the first summary, then observed the marker.
    objects
        .delete(&Path::from(metric_manifest_catalog_key(&manifests[1])))
        .await
        .unwrap();
    let catalog = MetricManifestCatalog::new(
        manifests
            .iter()
            .map(MetricManifestReference::from)
            .collect(),
    );
    objects
        .put(
            &Path::from(format!("{directory}/_published")),
            catalog.encode().unwrap().into(),
        )
        .await
        .unwrap();
    assert!(
        service
            .series("tenant-a", &[], 0, 21_000)
            .await
            .unwrap()
            .len()
            == 2
    );
    objects
        .delete(&Path::from(manifests[0].index_key.as_str()))
        .await
        .unwrap();
    let result = service.series("tenant-a", &[], 0, 22_000).await.unwrap();
    assert!(result.len() == 1);
    assert!(result[0].get("job").unwrap() == "job-1");
}

#[tokio::test]
async fn an_unbounded_snapshot_does_not_claim_to_cover_history_outside_its_lookback() {
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&objects), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&objects));
    write_float_manifest(
        &writer,
        &sink,
        "tenant-a",
        "historical",
        10_000,
        "metrics/tenant-a/float/history.parquet",
        0,
    )
    .await;
    let service = RefreshingMetricBlockStore::new(objects, base, "metrics", WalHead::new())
        .with_unbounded_compatibility_lookback(secs(1));
    assert!(
        service
            .series("tenant-a", &[], i64::MIN, i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        service
            .series("tenant-a", &[], 0, 20_000)
            .await
            .unwrap()
            .len()
            == 1
    );
}

#[tokio::test]
async fn catalog_retirement_during_get_relists_replacement_manifests() {
    use krabka_metrics::manifest_catalog::metric_manifest_catalog_key;

    use super::{ManifestObjectReplacement, ManifestRetirement};

    let counted = Arc::new(CountingObjectStore::new(Arc::default(), Time::ZERO));
    let objects: Arc<dyn ObjectStore> = counted.clone();
    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&objects), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&objects));
    let mut manifests = Vec::new();
    for (job, timestamp) in [("old", 10_000), ("replacement", 11_000)] {
        let key = format!("metrics/tenant-a/float/{job}.parquet");
        write_float_manifest(&writer, &sink, "tenant-a", job, timestamp, &key, 0).await;
        manifests.push(sink.read_manifest(&format!("{key}.index")).await.unwrap());
    }
    let replacement_key = Path::from(metric_manifest_catalog_key(&manifests[1]));
    let payload = objects
        .get(&replacement_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    objects.delete(&replacement_key).await.unwrap();
    // The first listing sees the old hint. Before its GET completes, the
    // replacement publishes, the old index retires, then cleanup removes its hint.
    *counted.retire_on_get.lock().unwrap() = Some(ManifestRetirement {
        key: Path::from(metric_manifest_catalog_key(&manifests[0])),
        replacement: Some(ManifestObjectReplacement {
            key: replacement_key,
            payload: payload.into(),
        }),
        also_delete: vec![Path::from(manifests[0].index_key.as_str())],
    });
    let service = RefreshingMetricBlockStore::new(objects, base, "metrics", WalHead::new());
    let result = service.series("tenant-a", &[], 0, 20_000).await.unwrap();
    assert!(result.len() == 1);
    assert!(result[0].get("job").unwrap() == "replacement");
}
