use super::*;

pub(crate) fn recompute_scan_nested_sets(
    batches: Vec<RecordBatch>,
    max_scan_concat: ByteSize,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    // `concat_batches` materialises every matched span into one RecordBatch.
    // Arrow's variable-length columns use i32 offsets, so a combined batch over
    // ~2 GiB overflows with an opaque `Offset overflow error`. Cap the merge at a
    // safe 1.5 GiB and surface an actionable error so a pathological query (an
    // unbounded time range over a huge tenant) degrades cleanly instead of
    // emitting `concat scan batches: Offset overflow error`.
    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let batches = align_scan_batches_to_schema(batches, &schema)?;
    let total: ByteSize = batches
        .iter()
        .map(|batch| {
            ByteSize::from_bytes(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX))
        })
        .sum();
    if total > max_scan_concat {
        return Err(TraceqlError::Store(format!(
            "scan result too large to merge ({} bytes > {} cap); \
             narrow the query time range or selector",
            total.bytes_usize(),
            max_scan_concat.bytes_usize()
        )));
    }
    let batch = concat_batches(&schema, &batches)
        .map_err(|err| TraceqlError::Store(format!("concat scan batches: {err}")))?;
    recompute_batch_nested_sets(&batch).map(|batch| vec![batch])
}
