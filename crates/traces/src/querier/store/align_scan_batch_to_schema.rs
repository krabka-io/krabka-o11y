use super::{
    Array, RecordBatch, SchemaRef, TraceqlError, cast, promoted_attr_column,
    promoted_span_attr_from_field,
};

pub(crate) fn align_scan_batch_to_schema(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, TraceqlError> {
    crate::align_record_batch::align_record_batch(
        batch,
        schema,
        |field, column| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                cast(column, field.data_type()).map_err(|err| {
                    TraceqlError::Store(format!(
                        "cast column `{}` from {:?} to {:?}: {err}",
                        field.name(),
                        column.data_type(),
                        field.data_type()
                    ))
                })
            }
        },
        |field| {
            // Cold blocks and live batches can omit promoted columns; rebuild
            // them from the generic attribute lists before scanning.
            let attr = promoted_span_attr_from_field(field)
                .ok_or_else(|| TraceqlError::Store(format!("missing column `{}`", field.name())))?;
            promoted_attr_column(batch, &attr).map_err(|err| TraceqlError::Store(err.to_string()))
        },
        |err| TraceqlError::Store(format!("align scan batch schema: {err}")),
    )
}
