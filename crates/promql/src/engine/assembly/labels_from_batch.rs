use super::{Array, BinaryArray, Labels, RecordBatch, StringArray, leaf};

/// Reconstructs a [`Labels`] set from the string label columns of one row of a
/// planner-path output batch.
///
/// This function treats `Utf8` and `Binary` columns as labels and skips the
/// `timestamp`/`value` columns.
pub(crate) fn labels_from_batch(batch: &RecordBatch, row: usize) -> Labels {
    labels_from_batch_columns(batch, row, |name| {
        name == leaf::TIME_COLUMN || name == leaf::VALUE_COLUMN || name == leaf::SAMPLE_TIME_COLUMN
    })
}

/// Reconstructs a [`Labels`] set from the `Utf8` and `Binary` columns of one
/// row, skipping each column whose name `is_value_column` accepts.
pub(super) fn labels_from_batch_columns(
    batch: &RecordBatch,
    row: usize,
    is_value_column: impl Fn(&str) -> bool,
) -> Labels {
    let mut labels = Labels::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if is_value_column(field.name()) {
            continue;
        }
        if let Some(column) = batch.column(index).as_any().downcast_ref::<BinaryArray>()
            && !column.is_null(row)
        {
            labels.insert(
                field.name().clone(),
                crate::PromqlString::from(column.value(row).to_vec()),
            );
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
