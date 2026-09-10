use super::{Array, BTreeSet, FixedSizeBinaryArray, RecordBatch, SCOL_TRACE_ID, TracesError};

/// Folds `batch`'s trace ids into the set the block's bloom filter is built
/// from.
///
/// The set is the one thing a compaction keeps that grows with what it reads
/// rather than with one batch of it, and it has to: Tempo sizes a trace bloom
/// from the number of distinct traces it will hold, so the count has to be
/// known before the first bit is set. It is bounded by the traces the *output*
/// block holds, not by the rows the inputs carry.
///
/// # Errors
/// Returns [`TracesError::Block`] when the batch has no `trace_id` column, or
/// has one that is not `FixedSizeBinary`.
pub(crate) fn push_trace_ids(
    batch: &RecordBatch,
    traces: &mut BTreeSet<[u8; 16]>,
) -> Result<(), TracesError> {
    let trace_ids = batch
        .column_by_name(SCOL_TRACE_ID)
        .ok_or_else(|| TracesError::Block("compacted block missing trace_id".into()))?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TracesError::Block("trace_id is not FixedSizeBinary".into()))?;
    for row in 0..batch.num_rows() {
        if trace_ids.is_null(row) {
            continue;
        }
        let mut trace_id = [0_u8; 16];
        trace_id.copy_from_slice(trace_ids.value(row));
        traces.insert(trace_id);
    }
    Ok(())
}
