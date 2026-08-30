use super::{Arc, Int32Array, RecordBatch, TracesError};

pub(crate) fn replace_int32_columns(
    batch: &RecordBatch,
    replacements: &[(&str, Vec<i32>)],
) -> Result<RecordBatch, TracesError> {
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    for (name, values) in replacements {
        let idx = schema
            .column_with_name(name)
            .ok_or_else(|| TracesError::Block(format!("missing column {name}")))?
            .0;
        columns[idx] = Arc::new(Int32Array::from(values.clone()));
    }
    RecordBatch::try_new(schema, columns).map_err(|err| TracesError::Block(err.to_string()))
}
