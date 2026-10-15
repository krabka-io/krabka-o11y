//! Shared test helpers for this test area.
use super::*;

pub(crate) mod exemplar_batch {
use super::*;

pub(crate) fn exemplar_batch(row: ExemplarRow) -> RecordBatch {
    encode_exemplar_rows(&[row]).unwrap()
}
}

pub(crate) mod expected_stats {
use super::*;

pub(crate) struct ExpectedStat {
    pub(crate) name: &'static str,
    pub(crate) value: usize,
}

pub(crate) fn expected_stats(stats: &[ExpectedStat]) -> Vec<NamedTsdbStat> {
    stats
        .iter()
        .map(|stat| NamedTsdbStat {
            name: stat.name.to_string(),
            value: stat.value,
        })
        .collect()
}
}

pub(crate) mod instant_up_query {
use super::*;

/// The samples and annotations of one instant query.
pub(crate) struct InstantUpQuery {
    pub(crate) samples: Vec<InstantSample>,
    pub(crate) annotations: Annotations,
}

/// Runs the instant query `up` for `tenant-a` at `t = 1s` over `block_store`.
pub(crate) async fn instant_up_query(block_store: BlockStore) -> InstantUpQuery {
    let store = MetricBlockStore::new(block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let annotated = engine
        .query_instant_with_annotations(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = annotated.result else {
        panic!("expected instant vector");
    };
    InstantUpQuery {
        samples,
        annotations: annotated.annotations,
    }
}
}

pub(crate) mod sidecar_manifest {
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
            labels: series_labels.clone().into(),
        }],
    )
}
}

pub(crate) mod write_float_block {
use super::*;

pub(crate) async fn write_float_block(
    block_store: &mut BlockStore,
    object_key: &str,
    series_labels: &Labels,
    ts_ms: i64,
    value: f64,
) {
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[float_sample_row(fp, ts_ms, value, None)]).unwrap();
    let block_meta = block_store
        .writer()
        .write_block("tenant-a", object_key, float_sample_schema(), &[batch])
        .await
        .unwrap();
    block_store
        .index_mut()
        .add_series("tenant-a", fp, series_labels);
    block_store.index_mut().add_block(&block_meta);
}
}

