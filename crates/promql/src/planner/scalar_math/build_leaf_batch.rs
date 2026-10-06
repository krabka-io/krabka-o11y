use super::{
    Arc, ArrayRef, BinaryArray, Float64Array, Int64Array, Labels, PromqlError, RecordBatch, Result,
    Schema,
};

pub(crate) fn build_leaf_batch(
    schema: Arc<Schema>,
    label_names: &[String],
    rows: &[(Labels, i64, f64)],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        // `None` (NULL) for an ABSENT label; `Some(b"")` for a PRESENT-empty one.
        let values = rows
            .iter()
            .map(|(labels, _, _)| labels.get_value(name).map(crate::PromqlString::as_bytes))
            .collect::<Vec<Option<&[u8]>>>();
        columns.push(Arc::new(BinaryArray::from(values)));
    }
    columns.push(Arc::new(Float64Array::from_iter_values(
        rows.iter().map(|(_, _, value)| *value),
    )));
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|(_, ts_ms, _)| *ts_ms),
    )));
    RecordBatch::try_new(schema, columns).map_err(|error| PromqlError::Exec(error.to_string()))
}
