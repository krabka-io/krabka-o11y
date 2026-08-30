use super::*;

/// Reconstructs a [`Labels`] set from the string label columns of one row of a
/// planner-path output batch.
///
/// This function treats only `Utf8` columns as labels and skips the
/// `timestamp`/`value` columns.
pub(crate) fn labels_from_batch(batch: &RecordBatch, row: usize) -> Labels {
    let mut labels = Labels::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == leaf::TIME_COLUMN
            || field.name() == leaf::VALUE_COLUMN
            || field.name() == leaf::SAMPLE_TIME_COLUMN
        {
            continue;
        }
        if let Some(column) = batch.column(index).as_any().downcast_ref::<StringArray>() {
            // NULL -> the label is ABSENT (skip); a non-null value (including
            // `""`) -> the label is PRESENT with that value. This preserves the
            // present-empty-vs-absent distinction the leaf encodes, so the
            // reconstructed fingerprint matches the original series identity.
            if !column.is_null(row) {
                labels.insert(field.name().clone(), column.value(row).to_string());
            }
        }
    }
    labels
}
