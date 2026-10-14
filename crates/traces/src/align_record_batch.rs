use arrow::{
    array::ArrayRef,
    datatypes::{Field, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};

pub(crate) fn align_record_batch<E>(
    batch: &RecordBatch,
    schema: &SchemaRef,
    mut align_existing: impl FnMut(&Field, &ArrayRef) -> Result<ArrayRef, E>,
    mut build_missing: impl FnMut(&Field) -> Result<ArrayRef, E>,
    map_batch_error: impl FnOnce(ArrowError) -> E,
) -> Result<RecordBatch, E> {
    if batch.schema() == *schema {
        return Ok(batch.clone());
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = if let Some(column) = batch.column_by_name(field.name()) {
            align_existing(field, column)?
        } else {
            build_missing(field)?
        };
        columns.push(column);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(map_batch_error)
}
