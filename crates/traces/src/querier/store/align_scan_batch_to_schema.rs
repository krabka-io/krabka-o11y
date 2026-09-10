use super::{
    Array, RecordBatch, SchemaRef, TraceqlError, cast, promoted_attr_column,
    promoted_span_attr_from_field,
};

pub(crate) fn align_scan_batch_to_schema(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, TraceqlError> {
    if batch.schema() == *schema {
        return Ok(batch.clone());
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let Some(column) = batch.column_by_name(field.name()) else {
            // The scanned batches need not agree on the promoted attribute
            // columns: cold blocks written under different ingest flags, and
            // live-tier batches which carry none, meet here. Rebuild the
            // column from the generic attribute lists that hold the same
            // values rather than dropping the batch or the column.
            let attr = promoted_span_attr_from_field(field)
                .ok_or_else(|| TraceqlError::Store(format!("missing column `{}`", field.name())))?;
            columns.push(
                promoted_attr_column(batch, &attr)
                    .map_err(|err| TraceqlError::Store(err.to_string()))?,
            );
            continue;
        };
        if column.data_type() == field.data_type() {
            columns.push(column.clone());
        } else {
            columns.push(cast(column, field.data_type()).map_err(|err| {
                TraceqlError::Store(format!(
                    "cast column `{}` from {:?} to {:?}: {err}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ))
            })?);
        }
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|err| TraceqlError::Store(format!("align scan batch schema: {err}")))
}
