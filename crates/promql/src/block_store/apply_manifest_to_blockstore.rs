use super::*;

pub(crate) fn apply_manifest_to_blockstore(store: &mut BlockStore, manifest: &CompactionIndexManifest) {
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
    });
}
