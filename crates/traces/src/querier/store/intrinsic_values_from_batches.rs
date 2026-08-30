use super::{BTreeSet, RecordBatch, TraceqlError, TypedValue, collect_intrinsic_value};

pub(crate) fn intrinsic_values_from_batches(
    tag: &str,
    batches: &[RecordBatch],
) -> Result<Vec<TypedValue>, TraceqlError> {
    let mut values = BTreeSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            collect_intrinsic_value(batch, row, tag, &mut values)?;
        }
    }
    Ok(values
        .into_iter()
        .map(|(type_, value)| TypedValue { type_, value })
        .collect())
}
