use super::{Arc, Int32Array, RecordBatch, TracesError};

pub(crate) struct Int32ColumnReplacement<'a> {
    pub(crate) name: &'a str,
    pub(crate) values: Vec<i32>,
}

pub(crate) fn replace_int32_columns(
    batch: &RecordBatch,
    replacements: &[Int32ColumnReplacement<'_>],
) -> Result<RecordBatch, TracesError> {
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    for replacement in replacements {
        let Int32ColumnReplacement { name, values } = replacement;
        let idx = schema
            .column_with_name(name)
            .ok_or_else(|| TracesError::Block(format!("missing column {name}")))?
            .0;
        columns[idx] = Arc::new(Int32Array::from(values.clone()));
    }
    RecordBatch::try_new(schema, columns).map_err(|err| TracesError::Block(err.to_string()))
}
