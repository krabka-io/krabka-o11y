use super::{RecordBatch, SchemaRef, TraceqlError, Array, cast};

pub(crate) fn align_scan_batch_to_schema(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, TraceqlError> {
    if batch.schema() == *schema {
        return Ok(batch.clone());
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = batch
            .column_by_name(field.name())
            .ok_or_else(|| TraceqlError::Store(format!("missing column `{}`", field.name())))?;
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
