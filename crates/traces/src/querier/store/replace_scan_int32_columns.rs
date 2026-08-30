use super::*;

pub(crate) fn replace_scan_int32_columns(
    batch: &RecordBatch,
    replacements: &[(&str, Vec<i32>)],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    for (name, values) in replacements {
        let idx = schema
            .column_with_name(name)
            .ok_or_else(|| TraceqlError::Store(format!("missing column `{name}`")))?
            .0;
        columns[idx] = Arc::new(Int32Array::from(values.clone())) as ArrayRef;
    }
    RecordBatch::try_new(schema, columns)
        .map_err(|err| TraceqlError::Store(format!("replace scan nested-set columns: {err}")))
}
