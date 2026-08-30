use super::{
    Arc, ArrayRef, AsArray, PCOL_STACKTRACE_PARTITION, ProfileError, RecordBatch, UInt64Array,
    UInt64Type,
};

pub(crate) fn remap_partitions(
    batch: &RecordBatch,
    partition_base: u64,
) -> Result<RecordBatch, ProfileError> {
    let partition_idx = batch
        .schema()
        .column_with_name(PCOL_STACKTRACE_PARTITION)
        .ok_or_else(|| {
            ProfileError::Store(format!(
                "samples table missing {PCOL_STACKTRACE_PARTITION} column"
            ))
        })?
        .0;
    let partitions = batch.column(partition_idx).as_primitive::<UInt64Type>();
    let remapped = UInt64Array::from_iter_values(
        (0..batch.num_rows()).map(|row| partition_base | partitions.value(row)),
    );
    let mut columns = batch.columns().to_vec();
    columns[partition_idx] = Arc::new(remapped) as ArrayRef;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|err| ProfileError::Store(err.to_string()))
}
