use super::{
    BTreeMap, BlockDescriptor, BlockIndex, CompactorRunError, LabelIndex, ObjectPath, ObjectStore,
    SharedLogDeleteRequests, TenantLogIndexes, active_log_delete_filters_from_requests,
    insert_descriptor_labels, keep_log_rows_not_deleted, read_log_block_from_object_store,
    write_log_block_to_object_store,
};

#[cfg_attr(test, mutants::skip)]
pub(crate) async fn materialize_delete_requests_in_object_store_block_index(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    delete_requests: &SharedLogDeleteRequests,
    materialized_blocks: &mut BTreeMap<String, Option<BlockDescriptor>>,
) -> Result<Option<TenantLogIndexes>, CompactorRunError> {
    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    let mut changed = false;

    for block in block_index.blocks() {
        let object_key = block.key.object_key();
        if let Some(materialized) = materialized_blocks.get(&object_key) {
            match materialized {
                Some(descriptor) => {
                    if descriptor != block {
                        changed = true;
                    }
                    insert_descriptor_labels(
                        &mut next_label_index,
                        label_index,
                        tenant,
                        descriptor,
                    )?;
                    next_block_index.insert(descriptor.clone());
                }
                None => {
                    changed = true;
                }
            }
            continue;
        }

        let delete_filters =
            active_log_delete_filters_from_requests(delete_requests, tenant, block.key.time_range)?;
        let mut descriptor = block.clone();

        if delete_filters.is_empty() {
            insert_descriptor_labels(&mut next_label_index, label_index, tenant, &descriptor)?;
            next_block_index.insert(descriptor);
            continue;
        }

        let rows = read_log_block_from_object_store(store, prefix, &block.key).await?;
        let original_len = rows.len();
        let kept_rows = keep_log_rows_not_deleted(rows, tenant, label_index, &delete_filters)?;

        if kept_rows.len() != original_len {
            changed = true;
            if kept_rows.is_empty() {
                materialized_blocks.insert(object_key, None);
                continue;
            }
            descriptor =
                write_log_block_to_object_store(store, prefix, &block.key, kept_rows).await?;
            materialized_blocks.insert(object_key, Some(descriptor.clone()));
        }

        insert_descriptor_labels(&mut next_label_index, label_index, tenant, &descriptor)?;
        next_block_index.insert(descriptor);
    }

    Ok(changed.then_some(TenantLogIndexes {
        label_index: next_label_index,
        block_index: next_block_index,
    }))
}
