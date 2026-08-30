use super::{Arc, ArrayRef, Float64Array, Int64Array, Labels, PromqlError, RecordBatch, Result, Schema, StringArray};

pub(crate) fn build_leaf_batch(
    schema: Arc<Schema>,
    label_names: &[String],
    rows: &[(Labels, i64, f64)],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        // `None` (NULL) for an ABSENT label; `Some("")` for a PRESENT-empty one.
        let values = rows
            .iter()
            .map(|(labels, _, _)| labels.get(name).map(str::to_string))
            .collect::<Vec<Option<String>>>();
        columns.push(Arc::new(StringArray::from(values)));
    }
    columns.push(Arc::new(Float64Array::from_iter_values(
        rows.iter().map(|(_, _, value)| *value),
    )));
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|(_, ts_ms, _)| *ts_ms),
    )));
    RecordBatch::try_new(schema, columns).map_err(|error| PromqlError::Exec(error.to_string()))
}
