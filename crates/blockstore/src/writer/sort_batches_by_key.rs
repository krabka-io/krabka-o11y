use super::{
    Arc, BlockStoreError, RecordBatch, Result, SORT_KEY_OPTIONS, SchemaRef, SortColumn,
    UInt32Array, concat_batches, key_columns, lexsort_to_indices, take_record_batch,
};

/// `batches` merged into one batch ordered by `sort_key`.
///
/// Arrow's lexicographic sort is unstable, so the row's own position is
/// appended as a last key. That makes the order total and so makes the result
/// the stable one: rows equal on the whole declared key keep the order the
/// caller wrote them in, and a signal that relies on a secondary order the
/// declaration does not name -- spans within one trace, say -- does not have
/// it shuffled.
pub(crate) fn sort_batches_by_key(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    sort_key: &[String],
) -> Result<RecordBatch> {
    let combined = match batches {
        [only] => only.clone(),
        many => concat_batches(schema, many)
            .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))?,
    };

    let positions = u32::try_from(combined.num_rows()).map_err(|_| {
        BlockStoreError::InvalidBlock("block has more rows than a u32 row index".to_string())
    })?;
    let mut columns = key_columns(&combined, sort_key)?
        .into_iter()
        .map(|values| SortColumn {
            values,
            options: Some(SORT_KEY_OPTIONS),
        })
        .collect::<Vec<_>>();
    columns.push(SortColumn {
        values: Arc::new(UInt32Array::from_iter_values(0..positions)),
        options: Some(SORT_KEY_OPTIONS),
    });

    let indices = lexsort_to_indices(&columns, None)
        .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))?;

    take_record_batch(&combined, &indices)
        .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
}
