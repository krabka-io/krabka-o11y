use super::{ArrayRef, BlockBatchStream, RecordBatch, Result, Row, RowConverter, Rows, StreamExt};

/// One input of a merge, positioned at the next row it has to offer.
///
/// The cursor holds the batch it is reading and that batch's sort key in
/// Arrow's comparable row format, and nothing else: the rest of the block is
/// still on the object store, and the batches already consumed are already
/// dropped. That is the whole memory an input costs a merge.
pub(crate) struct RunCursor {
    stream: BlockBatchStream,
    current: Option<(RecordBatch, Rows)>,
    row: usize,
}

impl RunCursor {
    pub(crate) fn new(stream: BlockBatchStream) -> Self {
        Self {
            stream,
            current: None,
            row: 0,
        }
    }

    /// Pulls batches until the cursor has a row to offer, or the run ends.
    ///
    /// # Errors
    /// Returns whatever the underlying block stream failed with, and
    /// [`BlockStoreError::InvalidBlock`](crate::BlockStoreError::InvalidBlock)
    /// when a batch's key columns cannot be encoded for comparison.
    pub(crate) async fn load(
        &mut self,
        converter: &RowConverter,
        key_indices: &[usize],
    ) -> Result<()> {
        while self.current.is_none() {
            let Some(batch) = self.stream.next().await.transpose()? else {
                return Ok(());
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let columns = key_indices
                .iter()
                .map(|index| ArrayRef::clone(batch.column(*index)))
                .collect::<Vec<_>>();
            let rows = converter
                .convert_columns(&columns)
                .map_err(|err| crate::error::BlockStoreError::InvalidBlock(err.to_string()))?;
            self.current = Some((batch, rows));
            self.row = 0;
        }
        Ok(())
    }

    /// The key of the row the cursor is on, or `None` when the run is spent.
    pub(crate) fn head(&self) -> Option<Row<'_>> {
        self.current.as_ref().map(|(_, rows)| rows.row(self.row))
    }

    /// Rows left in the batch the cursor is reading.
    pub(crate) fn remaining(&self) -> usize {
        self.current
            .as_ref()
            .map_or(0, |(batch, _)| batch.num_rows() - self.row)
    }

    /// How many rows from the cursor sort at or below `limit`.
    ///
    /// The batch is in the declared order, so this is a binary search rather
    /// than a scan -- which is what makes the merge cost one comparison per
    /// *run* of rows it emits instead of one per row.
    pub(crate) fn rows_upto(&self, limit: &Row<'_>) -> usize {
        let Some((batch, rows)) = &self.current else {
            return 0;
        };
        let (mut low, mut high) = (self.row, batch.num_rows());
        while low < high {
            let middle = low + (high - low) / 2;
            if rows.row(middle) <= *limit {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low - self.row
    }

    /// Slices `count` rows off the cursor and advances past them.
    pub(crate) fn take(&mut self, count: usize) -> RecordBatch {
        let (batch, _) = self.current.as_ref().expect("the cursor has a batch");
        let slice = batch.slice(self.row, count);
        self.row += count;
        if self.row == batch.num_rows() {
            // The batch is spent: drop it here rather than at the next load,
            // so a run that is waiting on a slow input is not also holding the
            // batch it already handed over.
            self.current = None;
            self.row = 0;
        }
        slice
    }
}
