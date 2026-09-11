use super::{
    Array, BooleanArray, COL_SPAN_ID, COL_TRACE_ID, HashSet, RecordBatch, TraceqlError,
    filter_record_batch, fixed,
};

/// Collapse rows that describe the same span to the first one seen.
///
/// A scan reads the cold blocks and the hot tier over the *same* window, so a
/// span that has been flushed to a block while its hot copy is still inside the
/// live tier's retention arrives from both. The identity is
/// `(trace_id, span_id)`: OTLP makes `span_id` unique within a trace, so the
/// pair names one span across every tier and every block. It is the same key
/// [`super::deduplicate_trace_spans`] uses to merge the tiers for a by-id
/// lookup, and the same one the query frontend's search merge uses, so all
/// three read paths agree on what "the same span" means.
///
/// The span itself is never in doubt. OTLP exports a span once, when it ends,
/// so the copies are byte-for-byte the same span, whether they came from an
/// exporter retry or from a flush that left the hot copy in place. What can
/// disagree are the *derived* columns, because each tier computed them over
/// the rows it happened to hold: the nested-set columns, and the trace-level
/// ones (`rootServiceName`, `rootName`, trace start and duration).
///
/// Batches are visited in order and the first row for a key wins, so the
/// caller's ordering is the tie-break: cold blocks are appended before the hot
/// tier, and the settled copy survives -- the same preference
/// [`super::deduplicate_trace_spans`] applies on the by-id path, so a span does
/// not describe its trace one way through search and another through by-id.
/// The nested-set columns are rebuilt over the merged batch immediately after
/// this, which is also why the duplicates have to go first: two rows for one
/// span would double their parent's `childCount` and inflate the numbering for
/// the whole trace. The trace-level columns are not rebuilt here; compaction is
/// what settles those.
pub(crate) fn deduplicate_scan_batches(
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    let mut seen: HashSet<([u8; 16], [u8; 8])> = HashSet::new();
    let mut kept = Vec::with_capacity(batches.len());
    for batch in batches {
        let trace_ids = fixed(&batch, COL_TRACE_ID)?;
        let span_ids = fixed(&batch, COL_SPAN_ID)?;
        let mut keep = Vec::with_capacity(batch.num_rows());
        let mut any_dropped = false;
        for row in 0..batch.num_rows() {
            if trace_ids.is_null(row) || span_ids.is_null(row) {
                // An unkeyable row cannot be shown to be a duplicate of
                // anything, and dropping it would lose a span rather than a
                // copy. Keep it.
                keep.push(true);
                continue;
            }
            let mut trace_id = [0_u8; 16];
            trace_id.copy_from_slice(trace_ids.value(row));
            let mut span_id = [0_u8; 8];
            span_id.copy_from_slice(span_ids.value(row));
            let first = seen.insert((trace_id, span_id));
            any_dropped |= !first;
            keep.push(first);
        }
        if !any_dropped {
            kept.push(batch);
            continue;
        }
        let filtered = filter_record_batch(&batch, &BooleanArray::from(keep))
            .map_err(|err| TraceqlError::Store(format!("deduplicate scan batches: {err}")))?;
        if filtered.num_rows() > 0 {
            kept.push(filtered);
        }
    }
    Ok(kept)
}
