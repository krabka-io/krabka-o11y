use super::{
    ArrayRef, BlockStoreError, DynComparator, Ordering, RecordBatch, Result, SORT_KEY_OPTIONS,
    UInt32Array, key_columns, make_comparator, take,
};

/// Verifies, batch by batch, that rows arrive in the declared sort order.
///
/// A streaming writer cannot sort what it has already handed to Parquet, so
/// the order has to be established as the rows go past rather than fixed up
/// afterwards. The check is the same relation [`super::is_sorted_by_key`]
/// tests -- one comparison pass over the key columns, ascending, nulls first
/// -- carried across the boundary between calls by keeping the previous
/// batch's last key row.
///
/// That carried row is a one-row copy rather than a slice of the batch it came
/// from: a slice shares the batch's buffers and would keep every column of it
/// alive until the next batch arrived, which is exactly the resident-input
/// cost a streaming writer exists to avoid.
pub(crate) struct SortKeyCheck {
    sort_key: Vec<String>,
    tail: Option<Vec<ArrayRef>>,
}

impl SortKeyCheck {
    pub(crate) fn new(sort_key: &[String]) -> Self {
        Self {
            sort_key: sort_key.to_vec(),
            tail: None,
        }
    }

    /// Whether `batch` continues the declared order, given every batch already
    /// accepted.
    ///
    /// A batch that is out of order leaves the check unchanged, so the caller
    /// decides what a violation means -- an error for the streaming writer, a
    /// sort for the buffered one.
    ///
    /// # Errors
    /// Returns [`BlockStoreError::InvalidBlock`] when `batch` lacks a declared
    /// sort-key column or the key columns cannot be compared.
    pub(crate) fn accept(&mut self, batch: &RecordBatch) -> Result<bool> {
        if self.sort_key.is_empty() || batch.num_rows() == 0 {
            return Ok(true);
        }

        let columns = key_columns(batch, &self.sort_key)?;
        let within = comparators(&columns, &columns)?;
        for row in 1..batch.num_rows() {
            if compare(&within, row - 1, row) == Ordering::Greater {
                return Ok(false);
            }
        }

        if let Some(tail) = &self.tail {
            let across = comparators(tail, &columns)?;
            if compare(&across, 0, 0) == Ordering::Greater {
                return Ok(false);
            }
        }

        self.tail = Some(last_key_row(&columns, batch.num_rows() - 1)?);
        Ok(true)
    }
}

/// An owned one-row copy of `columns` at `row`.
fn last_key_row(columns: &[ArrayRef], row: usize) -> Result<Vec<ArrayRef>> {
    let index = u32::try_from(row).map_err(|_| {
        BlockStoreError::InvalidBlock("batch has more rows than a u32 row index".to_string())
    })?;
    let index = UInt32Array::from(vec![index]);
    columns
        .iter()
        .map(|column| {
            take(column.as_ref(), &index, None)
                .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
        })
        .collect()
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
