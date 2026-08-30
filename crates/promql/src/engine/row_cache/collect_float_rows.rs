use super::{ScanResult, Result, FloatRow, AsArray, UInt64Type, Int64Type, Float64Type, PromqlError};

pub(crate) async fn collect_float_rows(
    scan: ScanResult,
    table: &str,
    max_samples: usize,
) -> Result<Vec<FloatRow>> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT series_fingerprint, timestamp, value FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await?;
    let batches = dataframe.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        let fps = batch.column(0).as_primitive::<UInt64Type>();
        let timestamps = batch.column(1).as_primitive::<Int64Type>();
        let values = batch.column(2).as_primitive::<Float64Type>();
        for row in 0..batch.num_rows() {
            if rows.len() >= max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={max_samples}"
                )));
            }
            rows.push(FloatRow {
                fp: fps.value(row),
                ts_ms: timestamps.value(row),
                value: values.value(row),
            });
        }
    }
    Ok(rows)
}
