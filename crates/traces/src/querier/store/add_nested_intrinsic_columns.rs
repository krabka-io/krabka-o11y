use super::{RecordBatch, SpanMatcher, TraceqlError, add_nested_intrinsic_columns_to_batch};

pub(crate) fn add_nested_intrinsic_columns(
    batches: Vec<RecordBatch>,
    matchers: &[SpanMatcher],
    projection_matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    let mut columns = matchers.to_vec();
    columns.extend_from_slice(projection_matchers);
    batches
        .into_iter()
        .map(|batch| add_nested_intrinsic_columns_to_batch(&batch, &columns, matchers))
        .collect()
}
