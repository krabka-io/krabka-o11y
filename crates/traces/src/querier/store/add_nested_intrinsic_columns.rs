use super::*;

pub(crate) fn add_nested_intrinsic_columns(
    batches: Vec<RecordBatch>,
    matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    batches
        .into_iter()
        .map(|batch| add_nested_intrinsic_columns_to_batch(&batch, matchers))
        .collect()
}
