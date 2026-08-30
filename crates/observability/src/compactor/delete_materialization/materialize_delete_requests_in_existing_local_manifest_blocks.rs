use super::{
    BlockIndex, BlockStoreError, CompactorRunError, ErrorKind, FsPath, LabelIndex,
    SharedLogDeleteRequests, active_log_delete_filters_from_requests, active_log_delete_tenants,
    insert_descriptor_labels, is_deleted_log_entry, read_log_block, read_log_index_manifest,
    write_log_block, write_log_index_manifest,
};

#[cfg_attr(test, mutants::skip)]
pub(crate) fn materialize_delete_requests_in_existing_local_manifest_blocks(
    root: &FsPath,
    delete_requests: &SharedLogDeleteRequests,
) -> Result<(), CompactorRunError> {
    let (label_index, block_index) = match read_log_index_manifest(root) {
        Ok(indexes) => indexes,
        Err(BlockStoreError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    let active_tenants = active_log_delete_tenants(delete_requests)?;
    if active_tenants.is_empty() {
        return Ok(());
    }

    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    let mut changed = false;

    for block in block_index.blocks() {
        let tenant = &block.key.tenant;
        let delete_filters = if active_tenants.contains(tenant) {
            active_log_delete_filters_from_requests(delete_requests, tenant, block.key.time_range)?
        } else {
            Vec::new()
        };
        let mut descriptor = block.clone();

        if !delete_filters.is_empty() {
            let rows = read_log_block(root, &block.key)?;
            let original_len = rows.len();
            let mut kept_rows = Vec::with_capacity(original_len);
            for row in rows {
                let labels = label_index
                    .labels_for(tenant, row.series_fingerprint)
                    .ok_or_else(|| CompactorRunError::MissingSeriesLabels {
                        tenant: tenant.clone(),
                        fingerprint: row.series_fingerprint,
                    })?;
                if is_deleted_log_entry(
                    &delete_filters,
                    labels,
                    &row.line,
                    &row.structured_metadata,
                    row.timestamp_ns,
                ) {
                    continue;
                }
                kept_rows.push(row);
            }

            if kept_rows.len() != original_len {
                changed = true;
                if kept_rows.is_empty() {
                    continue;
                }
                descriptor = write_log_block(root, &block.key, kept_rows)?;
            }
        }

        insert_descriptor_labels(&mut next_label_index, &label_index, tenant, &descriptor)?;
        next_block_index.insert(descriptor);
    }

    if changed {
        write_log_index_manifest(root, &next_label_index, &next_block_index)?;
    }
    Ok(())
}
