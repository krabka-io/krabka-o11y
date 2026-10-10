use arrow::{array::GenericByteArray, datatypes::ByteArrayType};

use super::{Array, BinaryArray, RecordBatch, SeriesFingerprint, StringArray};

/// The reconstructed fingerprint of every row of a planner-path output batch.
///
/// `labels_at` reads one row's label set the way that batch's shape encodes it.
/// A row whose `Utf8` and `Binary` columns all repeat the previous row's carries the same
/// label set, so its fingerprint is reused rather than rebuilt. The operator
/// chain emits one series per batch — [`SeriesDivide`] splits it that way — so a
/// grid-driven batch of one series over N instants costs one reconstruction
/// rather than N.
///
/// The comparison covers every `Utf8` and `Binary` column, including any that `labels_at`
/// skips, so it can only ever decline a reuse that would have been valid. It
/// never claims one that would not.
///
/// [`SeriesDivide`]: crate::extension::series_divide::SeriesDivide
pub(crate) fn row_fingerprints(
    batch: &RecordBatch,
    labels_at: impl Fn(&RecordBatch, usize) -> crate::PromqlLabels,
) -> Vec<SeriesFingerprint> {
    let text_columns: Vec<&StringArray> = batch
        .columns()
        .iter()
        .filter_map(|column| column.as_any().downcast_ref::<StringArray>())
        .collect();
    let byte_columns: Vec<&BinaryArray> = batch
        .columns()
        .iter()
        .filter_map(|column| column.as_any().downcast_ref::<BinaryArray>())
        .collect();
    let mut out: Vec<SeriesFingerprint> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let reused = out.last().copied().filter(|_| {
            text_columns
                .iter()
                .all(|column| repeats_previous_row(column, row))
                && byte_columns
                    .iter()
                    .all(|column| repeats_previous_row(column, row))
        });
        out.push(reused.unwrap_or_else(|| labels_at(batch, row).fingerprint()));
    }
    out
}

/// Whether `column` holds the same value, or the same null, at `row` as at the
/// row before it. Row 0 compares with itself.
fn repeats_previous_row<T: ByteArrayType>(column: &GenericByteArray<T>, row: usize) -> bool
where
    T::Native: PartialEq,
{
    let previous = row.saturating_sub(1);
    match (column.is_null(previous), column.is_null(row)) {
        (true, true) => true,
        (false, false) => column.value(previous) == column.value(row),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{BinaryArray, RecordBatch, row_fingerprints};

    #[test]
    fn reuse_observes_raw_byte_changes_nullness_and_exact_repetitions() {
        let values: Vec<Option<&[u8]>> = vec![
            Some(&[0xff]),
            Some(&[0xff]),
            Some(&[0xfe]),
            Some("�".as_bytes()),
            None,
            Some(b""),
        ];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("raw", DataType::Binary, true)])),
            vec![Arc::new(BinaryArray::from(values))],
        )
        .unwrap();
        let actual = row_fingerprints(&batch, super::super::labels_from_batch);
        assert2::assert!(actual.len() == 6 && actual[0] == actual[1]);
        let unique = actual
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert2::assert!(unique.len() == 5);
    }
}
