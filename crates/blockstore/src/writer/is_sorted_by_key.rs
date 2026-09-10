use super::{
    ArrayRef, BlockStoreError, DynComparator, Ordering, RecordBatch, Result, SORT_KEY_OPTIONS,
    key_columns, make_comparator,
};

/// Whether `batches`, read end to end, are already ordered by `sort_key`.
///
/// One comparison pass and no allocation per row, so the cost on the path a
/// correct caller takes is a scan of the key columns rather than a sort.
pub(crate) fn is_sorted_by_key(batches: &[RecordBatch], sort_key: &[String]) -> Result<bool> {
    let mut previous: Option<&RecordBatch> = None;

    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let columns = key_columns(batch, sort_key)?;
        let within = comparators(&columns, &columns)?;
        for row in 1..batch.num_rows() {
            if compare(&within, row - 1, row) == Ordering::Greater {
                return Ok(false);
            }
        }

        if let Some(earlier) = previous {
            let earlier_columns = key_columns(earlier, sort_key)?;
            let across = comparators(&earlier_columns, &columns)?;
            if compare(&across, earlier.num_rows() - 1, 0) == Ordering::Greater {
                return Ok(false);
            }
        }
        previous = Some(batch);
    }

    Ok(true)
}

/// One comparator per key column, each comparing a `left` row to a `right` row.
fn comparators(left: &[ArrayRef], right: &[ArrayRef]) -> Result<Vec<DynComparator>> {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            make_comparator(left.as_ref(), right.as_ref(), SORT_KEY_OPTIONS)
                .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
        })
        .collect()
}

/// Lexicographic comparison of `left_row` against `right_row` across the key.
fn compare(comparators: &[DynComparator], left_row: usize, right_row: usize) -> Ordering {
    for comparator in comparators {
        match comparator(left_row, right_row) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    Ordering::Equal
}
