use super::{
    BlockIndex, BlockStoreError, LabelIndex, LogIndexManifest, ObjectPath, ObjectStore,
    ObjectStoreExt, TimeRange, instrument, log_tenant_index_shard_manifest_object_path,
};
use crate::index_snapshot::{list_index_snapshot_objects, snapshot_generation_from_path};

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %tenant, start_ns = shard_range.start_ns, end_ns = shard_range.end_ns),
    err
)]
/// Reads the newest immutable generation of a tenant's index shard.
///
/// # Errors
/// Returns an error for an absent shard, malformed metadata, failed I/O, or
/// 64 consecutive reads overtaken by snapshot pruning.
pub async fn read_tenant_log_index_shard_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let key = log_tenant_index_shard_manifest_object_path(prefix, tenant, shard_range);
    read_log_index_shard_snapshot_base(store, &key, tenant)
        .await?
        .map(|(_, labels, blocks)| (labels, blocks))
        .ok_or_else(|| {
            object_store::Error::NotFound {
                path: key.to_string(),
                source: "log index shard has no published generation".into(),
            }
            .into()
        })
}

pub(super) async fn read_log_index_shard_snapshot_base(
    store: &dyn ObjectStore,
    key: &ObjectPath,
    tenant: &str,
) -> Result<Option<(u64, LabelIndex, BlockIndex)>, BlockStoreError> {
    for _ in 0..64 {
        let Some(meta) = list_index_snapshot_objects(store, key.as_ref())
            .await
            .map_err(log_snapshot_error)?
            .pop()
        else {
            return Ok(None);
        };
        let generation =
            snapshot_generation_from_path(&meta.location).map_err(log_snapshot_error)?;
        match store.get(&meta.location).await {
            Ok(object) => {
                let manifest: LogIndexManifest = serde_json::from_slice(&object.bytes().await?)?;
                let (labels, blocks) = manifest.into_indexes_for_tenant(tenant)?;
                return Ok(Some((generation, labels, blocks)));
            }
            // The latest generation in this listing may have been pruned by a
            // publisher. Select again instead of treating the shard as empty.
            Err(object_store::Error::NotFound { .. }) => tokio::task::yield_now().await,
            Err(error) => return Err(error.into()),
        }
    }
    Err(object_store::Error::Generic {
        store: "log index shard",
        source: format!("manifest `{key}` was pruned during 64 consecutive reads").into(),
    }
    .into())
}

pub(super) fn log_snapshot_error(error: crate::BlockStoreError) -> BlockStoreError {
    object_store::Error::Generic {
        store: "log index shard",
        source: Box::new(error),
    }
    .into()
}
