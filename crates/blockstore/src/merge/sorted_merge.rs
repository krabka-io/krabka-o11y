use arrow::row::Row;

use super::{
    BlockBatchStream, BlockStoreError, RecordBatch, Result, RowConverter, RunCursor,
    SORT_KEY_OPTIONS, SchemaRef, SortField, concat_batches,
};

const NO_RUN: usize = usize::MAX;

struct RunHead<'a> {
    index: usize,
    key: Row<'a>,
}

/// A k-way merge of blocks that are each already in the declared sort order.
///
/// Concatenating the inputs and sorting the result is the same rows in the
/// same order, and it costs every input at once; merging them costs one batch
/// per input. The output is in the declared order by construction, so the
/// block writer it feeds has nothing to sort and nothing to buffer either.
///
/// Ties go to the earliest run, so rows the declared key does not separate
/// keep the order the inputs were listed in -- a merge is deterministic, and
/// a signal that relies on a secondary order the declaration does not name
/// does not have it shuffled.
pub struct SortedMerge {
    schema: SchemaRef,
    converter: RowConverter,
    key_indices: Vec<usize>,
    runs: Vec<RunCursor>,
    batch_rows: usize,
    // A tournament tree holds run indices. Updates do not copy or allocate keys.
    // After initialization, only the consumed run's path needs an update.
    heads: Vec<usize>,
    // Resume initialization here after a read error or a cancelled load.
    initialized: usize,
    pending: Option<usize>,
}

impl SortedMerge {
    /// Merges `runs`, each already ordered by `sort_key`, into batches of at
    /// most `batch_rows` rows.
    ///
    /// Every run must yield batches carrying `schema`; a compactor that has to
    /// reshape its inputs does it in the stream it hands over.
    ///
    /// # Errors
    /// Returns [`BlockStoreError::InvalidBlock`] when `sort_key` is empty or
    /// names a column `schema` does not carry, or when the key's types have no
    /// comparable row encoding.
    pub fn new(
        schema: SchemaRef,
        sort_key: &[String],
        runs: Vec<BlockBatchStream>,
        batch_rows: usize,
    ) -> Result<Self> {
        if sort_key.is_empty() {
            return Err(BlockStoreError::InvalidBlock(
                "a merge needs a sort key to merge on".to_string(),
            ));
        }
        let key_indices = sort_key
            .iter()
            .map(|name| {
                schema.index_of(name).map_err(|_| {
                    BlockStoreError::InvalidBlock(format!("missing sort-key column `{name}`"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let fields = key_indices
            .iter()
            .map(|index| {
                SortField::new_with_options(
                    schema.field(*index).data_type().clone(),
                    SORT_KEY_OPTIONS,
                )
            })
            .collect::<Vec<_>>();
        let converter = RowConverter::new(fields)
            .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))?;

        Ok(Self {
            schema,
            converter,
            key_indices,
            runs: runs.into_iter().map(RunCursor::new).collect(),
            batch_rows: batch_rows.max(1),
            heads: Vec::new(),
            initialized: 0,
            pending: None,
        })
    }

    /// The next merged batch, or `None` once every run is spent.
    ///
    /// # Errors
    /// Returns whatever a run failed with, and
    /// [`BlockStoreError::InvalidBlock`] when the merged slices cannot be
    /// concatenated.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut slices: Vec<RecordBatch> = Vec::new();
        let mut rows = 0;

        while rows < self.batch_rows {
            self.load_heads().await?;

            let Some(winner) = self.lowest_head() else {
                break;
            };
            let take = {
                // Everything the winning run holds below the next-lowest head
                // is already in order, so it goes out as one slice rather than
                // as a row at a time.
                let count = match self.next_lowest_head(winner) {
                    Some(limit) => self.runs[winner].rows_before(&limit.key, winner < limit.index),
                    None => self.runs[winner].remaining(),
                };
                count.clamp(1, self.batch_rows - rows)
            };
            rows += take;
            slices.push(self.runs[winner].take(take));
            // Defer the next read until the caller needs another slice.
            self.pending = Some(winner);
        }

        if slices.is_empty() {
            return Ok(None);
        }
        concat_batches(&self.schema, &slices)
            .map(Some)
            .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
    }

    async fn load_heads(&mut self) -> Result<()> {
        if self.heads.is_empty() {
            while self.initialized < self.runs.len() {
                self.runs[self.initialized]
                    .load(&self.converter, &self.key_indices)
                    .await?;
                self.initialized += 1;
            }
            let leaves = self.runs.len().next_power_of_two();
            self.heads = vec![NO_RUN; leaves * 2];
            for (index, run) in self.runs.iter().enumerate() {
                if run.head().is_some() {
                    self.heads[leaves + index] = index;
                }
            }
            for node in (1..leaves).rev() {
                self.heads[node] = self.lower(self.heads[node * 2], self.heads[node * 2 + 1]);
            }
        } else if let Some(index) = self.pending {
            // Keep pending until load succeeds, including across cancellation.
            self.runs[index]
                .load(&self.converter, &self.key_indices)
                .await?;
            let mut node = self.heads.len() / 2 + index;
            self.heads[node] = if self.runs[index].head().is_some() {
                index
            } else {
                NO_RUN
            };
            while node > 1 {
                node /= 2;
                self.heads[node] = self.lower(self.heads[node * 2], self.heads[node * 2 + 1]);
            }
            self.pending = None;
        }
        Ok(())
    }

    fn lower(&self, left: usize, right: usize) -> usize {
        if left == NO_RUN {
            return right;
        }
        if right == NO_RUN {
            return left;
        }
        let left_key = self.runs[left].head().expect("an active run has a head");
        let right_key = self.runs[right].head().expect("an active run has a head");
        if (left_key, left) <= (right_key, right) {
            left
        } else {
            right
        }
    }

    fn lowest_head(&self) -> Option<usize> {
        (self.heads[1] != NO_RUN).then_some(self.heads[1])
    }

    fn next_lowest_head(&self, winner: usize) -> Option<RunHead<'_>> {
        // The sibling subtrees on the winner's path cover every other run.
        let mut node = self.heads.len() / 2 + winner;
        let mut next = NO_RUN;
        while node > 1 {
            next = self.lower(next, self.heads[node ^ 1]);
            node /= 2;
        }
        (next != NO_RUN).then(|| RunHead {
            index: next,
            key: self.runs[next].head().expect("an active run has a head"),
        })
    }
}
