use super::{Array, RecordBatch, SeriesFingerprint, StringArray};

/// The reconstructed fingerprint of every row of a planner-path output batch.
///
/// `labels_at` reads one row's label set the way that batch's shape encodes it.
/// A row whose `Utf8` columns all repeat the previous row's carries the same
/// label set, so its fingerprint is reused rather than rebuilt. The operator
/// chain emits one series per batch — [`SeriesDivide`] splits it that way — so a
/// grid-driven batch of one series over N instants costs one reconstruction
/// rather than N.
///
/// The comparison covers every `Utf8` column, including any that `labels_at`
/// skips, so it can only ever decline a reuse that would have been valid. It
/// never claims one that would not.
///
/// [`SeriesDivide`]: crate::extension::series_divide::SeriesDivide
pub(crate) fn row_fingerprints(
    batch: &RecordBatch,
    labels_at: impl Fn(&RecordBatch, usize) -> krabka_blockstore::Labels,
) -> Vec<SeriesFingerprint> {
    let text_columns: Vec<&StringArray> = batch
        .columns()
        .iter()
        .filter_map(|column| column.as_any().downcast_ref::<StringArray>())
        .collect();
    let mut out: Vec<SeriesFingerprint> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let reused = out.last().copied().filter(|_| {
            text_columns.iter().all(|column| {
                let previous = row.saturating_sub(1);
                match (column.is_null(previous), column.is_null(row)) {
                    (true, true) => true,
                    (false, false) => column.value(previous) == column.value(row),
                    _ => false,
                }
            })
        });
        out.push(reused.unwrap_or_else(|| labels_at(batch, row).fingerprint()));
    }
    out
}
