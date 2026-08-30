use super::*;

pub(crate) fn align_scan_batches_to_schema(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    batches
        .into_iter()
        .map(|batch| align_scan_batch_to_schema(&batch, schema))
        .collect()
}
