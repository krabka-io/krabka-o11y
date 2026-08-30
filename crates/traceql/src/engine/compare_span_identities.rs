use super::*;

pub(crate) fn compare_span_identities(batches: &[RecordBatch]) -> Result<HashSet<([u8; 16], [u8; 8])>> {
    let mut identities = HashSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            identities.insert((
                fixed_16(batch, COL_TRACE_ID, row)?,
                fixed_8(batch, COL_SPAN_ID, row)?,
            ));
        }
    }
    Ok(identities)
}
