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
    if batch.schema() == *schema {
        return Ok(batch.clone());
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if let Some(column) = batch.column_by_name(field.name()) {
            columns.push(column.clone());
            continue;
        }
        let attr = promoted_span_attr_from_field(field).ok_or_else(|| {
            TracesError::Block(format!("span block is missing column `{}`", field.name()))
        })?;
        columns.push(promoted_attr_column(batch, &attr)?);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|err| TracesError::Block(err.to_string()))
}
