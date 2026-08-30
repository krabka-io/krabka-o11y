use super::{RecordBatch, ShardedTraceBloom, TracesError, BTreeSet, Array, SCOL_TRACE_ID, FixedSizeBinaryArray};

pub(crate) fn trace_bloom(batches: &[RecordBatch]) -> Result<ShardedTraceBloom, TracesError> {
    let mut traces = BTreeSet::new();
    for batch in batches {
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
    }

    let mut bloom = ShardedTraceBloom::with_tempo_defaults(traces.len());
    for trace_id in traces {
        bloom.insert(&trace_id);
    }
    Ok(bloom)
}
