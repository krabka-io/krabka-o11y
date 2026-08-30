use super::{BlockWriter, TraceIndex, Arc, ObjectStore, BlockBuilderConfig, BTreeMap, PartitionWindow, TracesError, tenants_in_records, build_blocks_with_options, BlockBuildOptions};

/// Flush decoded partition windows and durably save the trace index.
///
/// This function returns the number of span blocks it durably wrote. The caller
/// should commit WAL offsets only after this returns `Ok(_)`.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn flush_partition_windows(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_store: Arc<dyn ObjectStore>,
    config: &BlockBuilderConfig,
    windows: BTreeMap<i32, PartitionWindow>,
) -> Result<usize, TracesError> {
    let mut blocks_written = 0usize;
    for (partition, partition_window) in windows {
        for tenant in tenants_in_records(&partition_window.records) {
            let metas = build_blocks_with_options(
                writer,
                index,
                &tenant,
                partition,
                &partition_window.records,
                partition_window.offset_range,
                BlockBuildOptions {
                    object_key_prefix: &config.object_key_prefix,
                    promoted_attrs: &config.promoted_attrs,
                },
            )
            .await?;
            blocks_written += metas.len();
        }
    }
    index
        .save_latest_snapshot_with_retain(
            &object_store,
            &config.index_key,
            config.index_snapshot_retain,
        )
        .await
        .map(|_| blocks_written)
        .map_err(|err| TracesError::Block(err.to_string()))
}
