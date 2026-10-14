use super::{COL_SPAN_ID, COL_TRACE_ID, HashSet, RecordBatch, Result, fixed_8, fixed_16};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpanIdentity {
    pub(crate) trace_id: [u8; 16],
    pub(crate) span_id: [u8; 8],
}

pub(crate) fn compare_span_identities(batches: &[RecordBatch]) -> Result<HashSet<SpanIdentity>> {
    let mut identities = HashSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            identities.insert(SpanIdentity {
                trace_id: fixed_16(batch, COL_TRACE_ID, row)?,
                span_id: fixed_8(batch, COL_SPAN_ID, row)?,
            });
        }
    }
    Ok(identities)
}
