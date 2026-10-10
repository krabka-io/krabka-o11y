use super::*;

/// One compacted `tenant-a` block for [`manifest_store`].
pub(crate) struct ManifestBlock {
    pub(crate) kind: MetricBlockKind,
    /// The block object key; its index key swaps `.parquet` for `.index`.
    pub(crate) block_key: &'static str,
    pub(crate) schema: SchemaRef,
    pub(crate) batch: RecordBatch,
    /// The last WAL offset the block covers, from offset 0.
    pub(crate) last_offset: i64,
    /// The one series the block holds.
    pub(crate) series_labels: Labels,
}

/// Writes `block` and returns a store rebuilt from its compaction manifest
/// over a fresh block store, as a restarted reader would load it.
pub(crate) async fn manifest_store(block: ManifestBlock) -> MetricBlockStore {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());
    let block_meta = writer_store
        .writer()
        .write_block("tenant-a", block.block_key, block.schema, &[block.batch])
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        block.kind,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: block.block_key.replace(".parquet", ".index"),
            first_offset: 0,
            last_offset: block.last_offset,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: block.series_labels.fingerprint(),
            labels: block.series_labels.into(),
        }],
    );

    let fresh_store = BlockStore::new(object_store, base);
    MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest])
}

/// The `job="api"` exemplars `store` holds for `tenant-a` over `[10s, 11s]`.
pub(crate) async fn api_exemplars(store: &MetricBlockStore) -> Vec<crate::ExemplarRecord> {
    store
        .exemplars(
            "tenant-a",
            &[crate::PromqlMatcher {
                name: "job".to_string(),
                op: krabka_blockstore::MatchOp::Eq,
                value: "api".to_string().into(),
            }],
            10_000,
            11_000,
        )
        .await
        .unwrap()
        .exemplars
}

/// Checks that the instant query `up` at 1s over `store` answers exactly the
/// float 1 for `series_labels`.
pub(crate) async fn assert_up_is_one(store: MetricBlockStore, series_labels: Labels) {
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels: series_labels.into(),
                ts_ms: 1_000,
                value: SampleValue::Float(1.0),
                drop_name: false,
            }]
    );
}
