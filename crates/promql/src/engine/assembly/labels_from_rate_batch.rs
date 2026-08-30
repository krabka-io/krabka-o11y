use super::{RecordBatch, Labels, rate_range, Array, StringArray};

/// Reconstructs a [`Labels`] set from the string label columns of one row of a
/// rate-range projection output batch.
///
/// The rate projection carries only label (`Utf8`) columns plus the float
/// `value` result column, so every non-`value` `Utf8` column is a label.
pub(crate) fn labels_from_rate_batch(batch: &RecordBatch, row: usize) -> Labels {
    let mut labels = Labels::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == rate_range::RATE_VALUE_COLUMN {
            continue;
        }
        if let Some(column) = batch.column(index).as_any().downcast_ref::<StringArray>() {
            // NULL -> absent (skip); any non-null value (including `""`) ->
            // present with that value. See `labels_from_batch`.
            if !column.is_null(row) {
                labels.insert(field.name().clone(), column.value(row).to_string());
            }
        }
    }
    labels
}
