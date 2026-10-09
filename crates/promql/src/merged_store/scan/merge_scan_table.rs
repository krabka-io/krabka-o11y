use std::collections::HashSet;

use arrow::{
    array::{AsArray, BooleanArray},
    compute::filter_record_batch,
    datatypes::{Int64Type, UInt64Type},
};
use futures::TryStreamExt as _;

use super::{Arc, COL_FINGERPRINT, COL_TIMESTAMP, MemTable, PromqlError, SessionContext};

pub(crate) async fn merge_scan_table<const N: usize>(
    ctx: &SessionContext,
    table_name: &str,
    schema: arrow::datatypes::SchemaRef,
    scans: [(SessionContext, Option<String>); N],
) -> Result<Option<String>, PromqlError> {
    // Read higher-priority sources first. Hot samples override cold samples
    // with the same fingerprint and timestamp, including stale markers.
    // One set spans every source and batch, so duplicates within a source
    // also appear only once. No SQL window sort or source aliases are needed.
    let mut seen = HashSet::<(u64, i64), ahash::RandomState>::default();
    let mut batches = Vec::new();
    for (scan_ctx, table) in scans.into_iter().rev() {
        let Some(table) = table else {
            continue;
        };
        let mut stream = scan_ctx.table(&table).await?.execute_stream().await?;
        while let Some(batch) = stream.try_next().await? {
            let fps = batch
                .column_by_name(COL_FINGERPRINT)
                .expect("metric schema has fingerprints")
                .as_primitive::<UInt64Type>();
            let timestamps = batch
                .column_by_name(COL_TIMESTAMP)
                .expect("metric schema has timestamps")
                .as_primitive::<Int64Type>();
            let keep = BooleanArray::from_iter(
                (0..batch.num_rows())
                    .map(|row| Some(seen.insert((fps.value(row), timestamps.value(row))))),
            );
            let batch = filter_record_batch(&batch, &keep)
                .map_err(|err| PromqlError::Exec(err.to_string()))?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
    }
    if batches.is_empty() {
        return Ok(None);
    }
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(Some(table_name.to_string()))
}
