use arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch};

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
    let gathered = match pending.as_slice() {
        [only] => only.clone(),
        many => arrow::compute::concat_batches(schema, many).map_err(map_concat_error)?,
    };
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
