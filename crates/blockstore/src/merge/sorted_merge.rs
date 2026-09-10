use super::{
    BlockBatchStream, BlockStoreError, RecordBatch, Result, RowConverter, RunCursor,
    SORT_KEY_OPTIONS, SchemaRef, SortField, concat_batches,
};

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
            for run in &mut self.runs {
                run.load(&self.converter, &self.key_indices).await?;
            }

            let Some(winner) = self.lowest_head() else {
                break;
            };
            let take = {
                // Everything the winning run holds below the next-lowest head
                // is already in order, so it goes out as one slice rather than
                // as a row at a time.
                let count = match self.next_lowest_head(winner) {
                    Some(limit) => self.runs[winner].rows_upto(&limit),
                    None => self.runs[winner].remaining(),
                };
                count.clamp(1, self.batch_rows - rows)
            };
            rows += take;
            slices.push(self.runs[winner].take(take));
        }

        if slices.is_empty() {
            return Ok(None);
        }
        concat_batches(&self.schema, &slices)
            .map(Some)
            .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
    }

    /// The run holding the lowest key, ties going to the earliest run.
    fn lowest_head(&self) -> Option<usize> {
        self.runs
            .iter()
            .enumerate()
            .filter_map(|(index, run)| run.head().map(|head| (index, head)))
            .min_by(|(_, left), (_, right)| left.cmp(right))
            .map(|(index, _)| index)
    }

    /// The lowest key held by any run other than `winner`.
    fn next_lowest_head(&self, winner: usize) -> Option<arrow::row::Row<'_>> {
        self.runs
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != winner)
            .filter_map(|(_, run)| run.head())
            .min()
    }
}
