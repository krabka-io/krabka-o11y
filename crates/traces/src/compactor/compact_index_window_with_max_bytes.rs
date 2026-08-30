use super::{
    Arc, BlockIndex, BlockMeta, BlockWriter, ByteSize, MaxOffset, MinOffset, ObjectStore,
    TraceIndex, TracesError, WindowStartNs, compact_block_keys_with_max_bytes,
    compacted_object_key, prefixed_object_key,
};

/// Compact every tenant using a caller-supplied on-disk block-read limit.
///
/// # Errors
/// Returns an error when an input exceeds the configured cap, the query is
/// malformed, an expression has incompatible operand types, or the backing
/// span store fails.
pub async fn compact_index_window_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    start_ns: i64,
    end_ns: i64,
    block_read_max: ByteSize,
) -> Result<Vec<BlockMeta>, TracesError> {
    let mut metas = Vec::new();
    for tenant in index.tenants() {
        let candidate_keys = index.candidate_blocks(&tenant, start_ns, end_ns);
        if candidate_keys.len() < 2 {
            continue;
        }
        let output_key = prefixed_object_key(
            object_key_prefix,
            &compacted_object_key(
                &tenant,
                0,
                MinOffset(0),
                MaxOffset(i64::try_from(candidate_keys.len()).unwrap_or(i64::MAX)),
                WindowStartNs(start_ns),
            ),
        );
        let meta = compact_block_keys_with_max_bytes(
            store.clone(),
            writer,
            index,
            &tenant,
            &candidate_keys,
            &output_key,
            block_read_max,
        )
        .await?;
        metas.push(meta);
    }
    Ok(metas)
}
