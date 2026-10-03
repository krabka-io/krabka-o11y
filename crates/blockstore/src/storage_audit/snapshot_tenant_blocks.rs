use super::{Arc, BlockStoreError, ByteSize, ObjectStore, ProfileIndex, StorageSignal, TraceIndex};

/// The block keys that the snapshot index at `key` names for `tenant`.
///
/// # Errors
/// Returns the loader's error when a payload of the tenant does not read or
/// decode.
pub async fn snapshot_tenant_blocks(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    key: &str,
    tenant: &str,
    max_bytes: ByteSize,
) -> Result<Vec<String>, BlockStoreError> {
    match signal {
        StorageSignal::Traces => {
            let index = TraceIndex::load_latest_snapshot_for_range_with_max_bytes(
                store,
                key,
                tenant,
                i64::MIN,
                i64::MAX,
                max_bytes,
            )
            .await?;
            Ok(index
                .trace_blocks(tenant)
                .iter()
                .map(|block| block.object_key.clone())
                .collect())
        }
        StorageSignal::Profiles => {
            let index = ProfileIndex::load_latest_snapshot_for_range_with_max_bytes(
                store,
                key,
                tenant,
                i64::MIN,
                i64::MAX,
                max_bytes,
            )
            .await?;
            Ok(index
                .all_blocks()
                .into_iter()
                .filter(|block| block.tenant == tenant)
                .map(|block| block.object_key)
                .collect())
        }
        StorageSignal::Metrics | StorageSignal::Logs => Ok(Vec::new()),
    }
}
