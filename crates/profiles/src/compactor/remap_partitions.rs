use super::*;

pub(crate) fn remap_partitions(
    batch: &RecordBatch,
    partition_map: &BTreeMap<u64, u64>,
) -> Result<RecordBatch, ProfilesError> {
    let partition_idx = batch
        .schema()
        .column_with_name(PCOL_STACKTRACE_PARTITION)
        .ok_or_else(|| {
            ProfilesError::Block(format!("missing `{PCOL_STACKTRACE_PARTITION}` column"))
        })?
        .0;
    let partitions = batch.column(partition_idx).as_primitive::<UInt64Type>();
    let remapped = UInt64Array::from_iter_values((0..batch.num_rows()).map(|row| {
        let partition = partitions.value(row);
        partition_map.get(&partition).copied().unwrap_or(partition)
    }));
    let mut columns = batch.columns().to_vec();
    columns[partition_idx] = Arc::new(remapped) as ArrayRef;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|err| ProfilesError::Block(err.to_string()))
}
