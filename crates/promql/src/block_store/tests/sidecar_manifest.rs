use super::*;

pub(crate) async fn sidecar_manifest(
    block_store: &BlockStore,
    kind: MetricBlockKind,
    object_key: &str,
    schema: SchemaRef,
    batch: RecordBatch,
    series_labels: &Labels,
) -> CompactionIndexManifest {
    let block_meta = block_store
        .writer()
        .write_block("tenant-a", object_key, schema, &[batch])
        .await
        .unwrap();
    CompactionIndexManifest::from_block_meta(
        kind,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: format!("{object_key}.index"),
            first_offset: 0,
            last_offset: 0,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: series_labels.fingerprint(),
            labels: series_labels.clone(),
        }],
    )
}
