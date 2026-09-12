use super::{
    BTreeMap, BlockDeletion, BlockDescriptor, BlockStoreError, CompactorRunError, ObjectPath,
    ObjectStore, SharedLogDeleteRequests, active_log_delete_tenants, delete_blocks,
    log_block_deletion, materialize_delete_requests_in_object_store_block_index,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    write_tenant_log_index_manifest_to_object_store, write_tenant_log_index_shard_to_object_store,
};

pub(crate) async fn materialize_delete_requests_in_existing_object_store_blocks(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    delete_requests: &SharedLogDeleteRequests,
) -> Result<(), CompactorRunError> {
    for tenant in active_log_delete_tenants(delete_requests)? {
        let mut materialized_blocks: BTreeMap<String, Option<BlockDescriptor>> = BTreeMap::new();

        match read_tenant_log_index_manifest_from_object_store(store, prefix, &tenant).await {
            Ok((label_index, block_index)) => {
                if let Some((next_label_index, next_block_index)) =
                    materialize_delete_requests_in_object_store_block_index(
                        store,
                        prefix,
                        &tenant,
                        &label_index,
                        &block_index,
                        delete_requests,
                        &mut materialized_blocks,
                    )
                    .await?
                {
                    write_tenant_log_index_manifest_to_object_store(
                        store,
                        prefix,
                        &tenant,
                        &next_label_index,
                        &next_block_index,
                    )
                    .await?;
                }
            }
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {}
            Err(error) => return Err(error.into()),
        }

        let shard_ranges = match read_tenant_log_index_shard_ranges_from_object_store(
            store, prefix, &tenant,
        )
        .await
        {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                delete_emptied_block_objects(store, prefix, &materialized_blocks).await;
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        for shard_range in shard_ranges {
            let (label_index, block_index) =
                read_tenant_log_index_shard_from_object_store(store, prefix, &tenant, shard_range)
                    .await?;
            if let Some((next_label_index, next_block_index)) =
                materialize_delete_requests_in_object_store_block_index(
                    store,
                    prefix,
                    &tenant,
                    &label_index,
                    &block_index,
                    delete_requests,
                    &mut materialized_blocks,
                )
                .await?
            {
                write_tenant_log_index_shard_to_object_store(
                    store,
                    prefix,
                    &tenant,
                    shard_range,
                    &next_label_index,
                    &next_block_index,
                )
                .await?;
            }
        }

        // Every index of the tenant is written by now, so an object that no
        // descriptor names any more is unreachable and can go. A block the
        // delete requests emptied is exactly that: the manifest rewrite above
        // dropped its descriptor, and nothing else names the object.
        delete_emptied_block_objects(store, prefix, &materialized_blocks).await;
    }
    Ok(())
}

/// Deletes the objects of the blocks that a delete request emptied.
///
/// A block a delete request only shortened keeps its object, because the
/// rewrite put the kept rows back under the same key. A block it emptied has
/// no rewrite and no descriptor, so leaving the object would orphan it for as
/// long as the bucket lives.
async fn delete_emptied_block_objects(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    materialized_blocks: &BTreeMap<String, Option<BlockDescriptor>>,
) {
    let deletions: Vec<BlockDeletion> = materialized_blocks
        .iter()
        .filter(|(_, descriptor)| descriptor.is_none())
        .map(|(object_key, _)| log_block_deletion(prefix, object_key))
        .collect();
    if deletions.is_empty() {
        return;
    }
    let report = delete_blocks(store, &deletions).await;
    for failure in &report.failures {
        tracing::warn!(
            object = %failure.failed_key,
            error = %failure.error,
            "log delete materialization could not delete an emptied block object"
        );
    }
}
