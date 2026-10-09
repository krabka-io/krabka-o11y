use krabka_metrics::{
    FloatRow, ObjectStoreCompactionIndexSink, TenantCompactionRows, list_compaction_manifests,
    write_compacted_tenant_blocks,
};

use super::*;

/// Writes one float block per offset in `offsets`, each with one sample of
/// `soak_metric{block="<offset>"}`.
async fn write_blocks(object_store: &Arc<dyn ObjectStore>, offsets: &[i64]) {
    let writer = krabka_blockstore::BlockWriter::new(Arc::clone(object_store));
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(object_store));
    for &offset in offsets {
        let series = labels(&[("__name__", "soak_metric"), ("block", &offset.to_string())]);
        let rows = TenantCompactionRows {
            tenant: "tenant-a".to_string(),
            series_labels: std::collections::BTreeMap::from([(
                series.fingerprint(),
                series.clone().into(),
            )]),
            float_rows: vec![FloatRow {
                fingerprint: series.fingerprint(),
                timestamp_ms: 90_000,
                value: 1.0,
                start_timestamp_ms: None,
            }],
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        };
        write_compacted_tenant_blocks(&writer, &sink, &rows, offset, offset)
            .await
            .unwrap();
    }
}

#[tokio::test]
pub(crate) async fn a_block_deleted_after_the_manifest_listing_is_skipped_and_warned() {
    // `warm` runs one query before the delete, so the deleted block's footer
    // is in the footer cache when the second query reads it.
    for (query, warm) in [
        "count(count_over_time(soak_metric[60s]))",
        "sum(last_over_time(soak_metric[60s]))",
        "avg(last_over_time(soak_metric[60s]))",
    ]
    .into_iter()
    .flat_map(|query| [false, true].map(|warm| (query, warm)))
    {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write_blocks(&object_store, &[1, 2]).await;
        let manifests = list_compaction_manifests(&object_store).await.unwrap();
        let deleted = manifests
            .iter()
            .find(|manifest| manifest.first_offset == 1)
            .unwrap()
            .block_key
            .clone();
        let base = url::Url::parse("memory:///").unwrap();
        let store = MetricBlockStore::from_compaction_manifests(
            BlockStore::new(Arc::clone(&object_store), base.clone()),
            Some(BlockStore::new(Arc::clone(&object_store), base)),
            &manifests,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        if warm {
            engine
                .query_instant(&tenant_id("tenant-a"), query, 100_000)
                .await
                .unwrap();
        }

        object_store
            .delete(&ObjectPath::from(deleted.as_str()))
            .await
            .unwrap();
        let (result, annotations) = engine
            .query_instant_with_annotations(&tenant_id("tenant-a"), query, 100_000)
            .await
            .unwrap();

        check!(
            (result, annotations)
                == (
                    QueryResult::InstantVector(vec![InstantSample {
                        labels: Labels::new().into(),
                        ts_ms: 100_000,
                        value: SampleValue::Float(1.0),
                        drop_name: false,
                    }]),
                    Annotations {
                        // The engine appends the source position of the
                        // selector that the warning came from.
                        warnings: vec![format!(
                            "block \"{deleted}\" is missing from object storage and is not in \
                             this result (1:1)"
                        )],
                        infos: Vec::new(),
                        ..crate::Annotations::default()
                    }
                ),
            "query={query}, warm={warm}"
        );
    }
}
