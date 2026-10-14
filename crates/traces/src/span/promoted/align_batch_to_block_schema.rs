use super::{
    RecordBatch, SchemaRef, TracesError, promoted_attr_column, promoted_span_attr_from_field,
};

/// Project a span-block batch onto `schema`, rebuilding any promoted
/// attribute column the batch does not already carry.
///
/// Compaction merges blocks written under different promoted-attribute
/// configurations, and the writer requires every batch to match the output
/// schema exactly. Columns are matched by name, so an input keeps its own
/// values wherever it has them.
pub(crate) fn align_batch_to_block_schema(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, TracesError> {
    crate::align_record_batch::align_record_batch(
        batch,
        schema,
        |_field, column| Ok(column.clone()),
        |field| {
            let attr = promoted_span_attr_from_field(field).ok_or_else(|| {
                TracesError::Block(format!("span block is missing column `{}`", field.name()))
            })?;
            promoted_attr_column(batch, &attr)
        },
        |err| TracesError::Block(err.to_string()),
    )
}
