use arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch};

/// Buffers Arrow batches until a caller-defined boundary can be emitted.
pub struct BufferedRecordBatches {
    schema: SchemaRef,
    pending: Vec<RecordBatch>,
    rows: usize,
}

impl BufferedRecordBatches {
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            pending: Vec::new(),
            rows: 0,
        }
    }

    pub fn push(&mut self, batch: RecordBatch) {
        push_buffered_batch(&mut self.pending, &mut self.rows, batch);
    }

    pub fn take_complete<E>(
        &mut self,
        min_rows: usize,
        last_group_start: impl FnOnce(&RecordBatch) -> Result<usize, E>,
        map_concat_error: impl FnOnce(ArrowError) -> E,
    ) -> Result<Option<RecordBatch>, E> {
        take_complete_batch(
            &self.schema,
            &mut self.pending,
            &mut self.rows,
            min_rows,
            last_group_start,
            map_concat_error,
        )
    }

    pub fn take_rest<E>(
        &mut self,
        map_concat_error: impl FnOnce(ArrowError) -> E,
    ) -> Result<Option<RecordBatch>, E> {
        take_buffered_rest(
            &self.schema,
            &mut self.pending,
            &mut self.rows,
            map_concat_error,
        )
    }
}

/// Takes the prefix of buffered batches that contains complete adjacent groups.
///
/// `last_group_start` returns the row where the final, possibly incomplete
/// group starts. Rows from that point remain buffered for the next call.
pub fn take_complete_batch<E>(
    schema: &SchemaRef,
    pending: &mut Vec<RecordBatch>,
    rows: &mut usize,
    min_rows: usize,
    last_group_start: impl FnOnce(&RecordBatch) -> Result<usize, E>,
    map_concat_error: impl FnOnce(ArrowError) -> E,
) -> Result<Option<RecordBatch>, E> {
    if *rows < min_rows {
        return Ok(None);
    }
    let gathered = gather_batches(schema, pending, map_concat_error)?;
    pending.clear();
    let cut = last_group_start(&gathered)?;
    if cut == 0 {
        pending.push(gathered);
        return Ok(None);
    }
    *rows = gathered.num_rows() - cut;
    pending.push(gathered.slice(cut, *rows));
    Ok(Some(gathered.slice(0, cut)))
}

/// Adds an Arrow batch to a row-counted group buffer.
pub fn push_buffered_batch(pending: &mut Vec<RecordBatch>, rows: &mut usize, batch: RecordBatch) {
    *rows += batch.num_rows();
    pending.push(batch);
}

/// Takes and clears every buffered row after its final group becomes complete.
pub fn take_buffered_rest<E>(
    schema: &SchemaRef,
    pending: &mut Vec<RecordBatch>,
    rows: &mut usize,
    map_concat_error: impl FnOnce(ArrowError) -> E,
) -> Result<Option<RecordBatch>, E> {
    if *rows == 0 {
        return Ok(None);
    }
    let gathered = gather_batches(schema, pending, map_concat_error)?;
    pending.clear();
    *rows = 0;
    Ok(Some(gathered))
}

fn gather_batches<E>(
    schema: &SchemaRef,
    pending: &[RecordBatch],
    map_concat_error: impl FnOnce(ArrowError) -> E,
) -> Result<RecordBatch, E> {
    match pending {
        [only] => Ok(only.clone()),
        many => arrow::compute::concat_batches(schema, many).map_err(map_concat_error),
    }
}
