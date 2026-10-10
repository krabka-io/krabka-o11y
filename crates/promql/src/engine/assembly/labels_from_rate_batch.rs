use super::{Labels, RecordBatch, labels_from_batch::labels_from_batch_columns, rate_range};

/// Reconstructs a [`Labels`] set from the string label columns of one row of a
/// rate-range projection output batch.
///
/// The rate projection carries only label (`Utf8`) columns plus the float
/// `value` result column, so every non-`value` `Utf8` column is a label.
pub(crate) fn labels_from_rate_batch(batch: &RecordBatch, row: usize) -> Labels {
    labels_from_batch_columns(batch, row, |name| name == rate_range::RATE_VALUE_COLUMN)
}
