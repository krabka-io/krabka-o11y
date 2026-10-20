use super::{
    Arc, ArrayRef, BinaryArray, Float64Array, Int64Array, Labels, PromqlError, RecordBatch, Result,
    Schema,
};

pub(crate) struct ScalarMathSampleRow {
    pub(crate) labels: Labels,
    pub(crate) timestamp_ms: i64,
    pub(crate) value: f64,
}

pub(crate) fn build_leaf_batch(
    schema: Arc<Schema>,
    label_names: &[String],
    rows: &[ScalarMathSampleRow],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        // `None` (NULL) for an ABSENT label; `Some(b"")` for a PRESENT-empty one.
        let values = rows
            .iter()
            .map(|row| {
                row.labels
                    .get_value(name)
                    .map(crate::PromqlString::as_bytes)
            })
            .collect::<Vec<Option<&[u8]>>>();
        columns.push(Arc::new(BinaryArray::from(values)));
    }
    columns.push(Arc::new(Float64Array::from_iter_values(
        rows.iter().map(|row| row.value),
    )));
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| row.timestamp_ms),
    )));
    RecordBatch::try_new(schema, columns).map_err(|error| PromqlError::Exec(error.to_string()))
}
