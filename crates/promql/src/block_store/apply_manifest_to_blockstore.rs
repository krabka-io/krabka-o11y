use super::{BlockMeta, BlockStore, CompactionIndexManifest};

pub(crate) fn apply_manifest_to_blockstore(
    store: &mut BlockStore,
    manifest: &CompactionIndexManifest,
) {
    for series in &manifest.series {
        store
            .index_mut()
            .add_series(&manifest.tenant, series.fingerprint, &series.labels);
    }
    store.index_mut().add_block(&BlockMeta {
        tenant: manifest.tenant.clone(),
        object_key: manifest.block_key.clone(),
        min_ts: manifest.min_ts,
        max_ts: manifest.max_ts,
        row_count: manifest.row_count,
        fingerprints: manifest.fingerprints.clone(),
        // The manifest is the metrics index, so the level it records is the
        // only place a rebuilt index can learn one. Stamping every block
        // `INGESTED` here would make the compactor re-merge its own output.
        level: manifest.level,
    });
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_blockstore::BlockLevel;
    use krabka_metrics::MetricBlockKind;

    use super::*;

    fn manifest(block_key: &str, level: BlockLevel) -> CompactionIndexManifest {
        CompactionIndexManifest {
            tenant: "tenant-a".to_string(),
            kind: MetricBlockKind::Float,
            block_key: block_key.to_string(),
            index_key: format!("{block_key}.index"),
            level,
            first_offset: 0,
            last_offset: 1,
            row_count: 2,
            min_ts: 1_000,
            max_ts: 2_000,
            fingerprints: vec![7],
            series: Vec::new(),
        }
    }

    /// The manifests are the metrics index, so the level a rebuilt index holds
    /// can only come from the manifest. A rebuild that stamped every block
    /// level zero would offer a compactor its own output as an input for as
    /// long as the process ran, and the planner's termination argument rests on
    /// the level rising.
    #[test]
    fn a_rebuilt_index_carries_the_level_each_manifest_records() {
        let mut store = BlockStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            url::Url::parse("memory:///").expect("base url"),
        );

        for (block_key, level) in [
            ("metrics/tenant-a/float/ingested.parquet", BlockLevel(0)),
            ("metrics/tenant-a/float/compacted/l1.parquet", BlockLevel(1)),
            ("metrics/tenant-a/float/compacted/l3.parquet", BlockLevel(3)),
        ] {
            apply_manifest_to_blockstore(&mut store, &manifest(block_key, level));

            check!(
                store.index().block_level(block_key) == Some(level),
                "{block_key}"
            );
        }
    }
}
