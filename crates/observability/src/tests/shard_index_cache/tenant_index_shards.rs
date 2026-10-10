use super::{
    Arc, BlockIndex, LabelIndex, ObjectPath, QuerierState, RecordingObjectStore, TimeRange,
};

/// A tenant's label and block indexes, written to the object store as one
/// index shard per range in `shard_ranges`.
pub(crate) struct TenantIndexShards<'a> {
    pub(crate) prefix: &'a ObjectPath,
    pub(crate) tenant: &'a str,
    pub(crate) shard_ranges: &'a [TimeRange],
    pub(crate) labels_index: &'a LabelIndex,
    pub(crate) block_index: &'a BlockIndex,
}

/// Writes `shards` to `store`, forgets the writes it recorded, and returns an
/// empty querier that loads tenant indexes from those shards on demand.
pub(crate) async fn querier_state_over_index_shards(
    store: &RecordingObjectStore,
    shards: TenantIndexShards<'_>,
) -> QuerierState {
    krabka_blockstore::write_tenant_log_index_shards_to_object_store(
        store,
        shards.prefix,
        shards.tenant,
        shards.shard_ranges,
        shards.labels_index,
        shards.block_index,
    )
    .await
    .unwrap();
    store.clear_recorded_paths();

    QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    )
    .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), shards.prefix.clone())
}

/// Where one tenant index shard lives in the object store.
#[derive(Clone, Copy)]
pub(crate) struct TenantIndexShard<'a> {
    pub(crate) prefix: &'a ObjectPath,
    pub(crate) tenant: &'a str,
    pub(crate) shard_range: TimeRange,
}

/// How many times `store` fetched the snapshot of `shard`.
pub(crate) fn shard_snapshot_get_count(
    store: &RecordingObjectStore,
    shard: TenantIndexShard<'_>,
) -> usize {
    let manifest = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
        shard.prefix,
        shard.tenant,
        shard.shard_range,
    );
    let snapshot = format!(
        "{}/00000000000000000000.json",
        krabka_blockstore::index_snapshot_prefix_for_key(manifest.as_ref())
    );
    store
        .get_paths()
        .into_iter()
        .filter(|path| path == &snapshot)
        .count()
}

/// How many times `store` listed the index shard prefix of `tenant`.
pub(crate) fn shard_prefix_list_count(
    store: &RecordingObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> usize {
    let shard_prefix =
        krabka_blockstore::log_tenant_index_shards_object_prefix(prefix, tenant).to_string();
    store
        .list_prefixes()
        .into_iter()
        .filter(|listed| listed == &shard_prefix)
        .count()
}
