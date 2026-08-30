use super::{ProfileError, ProfileScan, RecordBatch, UnionSymbols, remap_partitions};

pub(crate) async fn collect_and_remap(
    scan: ProfileScan,
    source_id: u64,
    symbols: &mut UnionSymbols,
) -> Result<Vec<RecordBatch>, ProfileError> {
    let partition_base = source_id << 56;
    symbols.insert(partition_base, scan.symbols);
    let sql = format!("SELECT * FROM {}", scan.samples_table);
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    batches
        .into_iter()
        .map(|batch| remap_partitions(&batch, partition_base))
        .collect()
}
