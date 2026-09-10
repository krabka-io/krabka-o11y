use super::{
    Arc, ArrayRef, Float64Array, Int64Array, LabeledSeries, PromqlError, RecordBatch, Result,
    Schema, StringBuilder,
};

/// Builds the leaf batch for one window's worth of matched series.
///
/// `series` arrive grouped, one entry per series, with their samples already in
/// timestamp order, so each label column is written run by run: one lookup per
/// series rather than one per sample.
pub(crate) fn build_leaf_batch(
    schema: Arc<Schema>,
    label_names: &[String],
    series: &[LabeledSeries],
) -> Result<RecordBatch> {
    let rows: usize = series.iter().map(|one| one.samples.len()).sum();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(label_names.len() + 3);
    for name in label_names {
        // `None` (NULL) for an ABSENT label; `Some("")` for a PRESENT-empty
        // label. The two must stay distinct so the reconstructed fingerprint
        // matches the original series identity.
        let mut builder = StringBuilder::with_capacity(rows, rows);
        for one in series {
            let value = one.labels.get(name);
            for _ in 0..one.samples.len() {
                builder.append_option(value);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }
    let timestamps = series
        .iter()
        .flat_map(|one| one.samples.iter().map(|sample| sample.ts_ms))
        .collect::<Vec<_>>();
    columns.push(Arc::new(Int64Array::from_iter_values(
        timestamps.iter().copied(),
    )));
    columns.push(Arc::new(Float64Array::from_iter_values(
        series
            .iter()
            .flat_map(|one| one.samples.iter().map(|sample| sample.value)),
    )));
    // Duplicate of the sample timestamp, carried through the chain unchanged.
    columns.push(Arc::new(Int64Array::from_iter_values(timestamps)));
    RecordBatch::try_new(schema, columns).map_err(|error| PromqlError::Exec(error.to_string()))
}
