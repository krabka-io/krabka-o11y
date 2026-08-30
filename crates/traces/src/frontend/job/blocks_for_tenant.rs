use super::{BlockMetaInfo, BlockStore, BlockStoreResult, RowGroupInfo, TraceIndex};

/// Read the block metadata for one tenant out of a `TraceIndex`, together with
/// the parquet row-group metadata.
///
/// This is ported from the legacy `backend_blocks_from_trace_index`.
///
/// # Errors
/// Propagates object-store and parquet read errors.
pub async fn blocks_for_tenant(
    blocks: &BlockStore,
    index: &TraceIndex,
    tenant: &str,
) -> BlockStoreResult<Vec<BlockMetaInfo>> {
    let mut out = Vec::new();
    for block in index.trace_blocks(tenant) {
        let row_groups: Vec<RowGroupInfo> = blocks
            .read_row_group_metadata(&block.object_key)
            .await?
            .into_iter()
            .filter_map(|rg| {
                let index = u32::try_from(rg.index).ok()?;
                Some(RowGroupInfo {
                    index,
                    compressed: rg.compressed,
                })
            })
            .collect();
        let size = row_groups.iter().map(|rg| rg.compressed).sum();
        out.push(BlockMetaInfo {
            block_id: block.object_key.clone(),
            start_ns: block.min_ts,
            end_ns: block.max_ts,
            size,
            row_groups,
        });
    }
    Ok(out)
}
