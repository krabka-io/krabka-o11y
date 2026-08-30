use super::*;

pub(crate) fn filter_batches_by_matchers(
    batches: Vec<RecordBatch>,
    matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    if matchers.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| {
            let mask = (0..batch.num_rows())
                .map(|row| row_matches(&batch, row, matchers))
                .collect::<Result<Vec<_>, _>>()?;
            filter_record_batch(&batch, &BooleanArray::from(mask))
                .map_err(|err| TraceqlError::Store(err.to_string()))
        })
        .collect()
}
