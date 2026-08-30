use super::{Arc, ArrayRef, Float64Array, Int64Array, LabeledSample, PromqlError, RecordBatch, Result, Schema, StringArray};

pub(crate) fn build_leaf_batch(
    schema: Arc<Schema>,
    label_names: &[String],
    rows: &[LabeledSample],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(label_names.len() + 3);
    for name in label_names {
        // `None` (NULL) for an ABSENT label; `Some("")` for a PRESENT-empty
        // label. The two must stay distinct so the reconstructed fingerprint
        // matches the original series identity.
        let values = rows
            .iter()
            .map(|row| row.labels.get(name).map(str::to_string))
            .collect::<Vec<Option<String>>>();
        columns.push(Arc::new(StringArray::from(values)));
    }
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| row.ts_ms),
    )));
    columns.push(Arc::new(Float64Array::from_iter_values(
        rows.iter().map(|row| row.value),
    )));
    // Duplicate of the sample timestamp, carried through the chain unchanged.
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| row.ts_ms),
    )));
    RecordBatch::try_new(schema, columns).map_err(|error| PromqlError::Exec(error.to_string()))
}
