use super::{Arc, ArrayRef, Int32Array, RecordBatch, TraceqlError};

pub(crate) struct ScanInt32ColumnReplacement<'a> {
    pub(crate) name: &'a str,
    pub(crate) values: Vec<i32>,
}

pub(crate) fn replace_scan_int32_columns(
    batch: &RecordBatch,
    replacements: &[ScanInt32ColumnReplacement<'_>],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    for replacement in replacements {
        let ScanInt32ColumnReplacement { name, values } = replacement;
        let idx = schema
            .column_with_name(name)
            .ok_or_else(|| TraceqlError::Store(format!("missing column `{name}`")))?
            .0;
        columns[idx] = Arc::new(Int32Array::from(values.clone())) as ArrayRef;
    }
    RecordBatch::try_new(schema, columns)
        .map_err(|err| TraceqlError::Store(format!("replace scan nested-set columns: {err}")))
}
