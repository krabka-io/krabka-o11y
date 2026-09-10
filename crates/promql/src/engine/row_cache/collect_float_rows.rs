use super::{
    AsArray, Float64Type, FloatRow, Int64Type, PromqlError, Result, ScanResult, UInt64Type,
};

pub(crate) async fn collect_float_rows(
    scan: ScanResult,
    table: &str,
    max_samples: usize,
) -> Result<Vec<FloatRow>> {
    // No `ORDER BY`. The store bounds the scan to the requested series and
    // window, and the only caller wraps the result in a `FloatWindow`, which
    // establishes `(series_fingerprint, timestamp)` order itself — and skips the
    // sort outright when the rows already arrive that way, which they do when a
    // single block answers the scan. A global sort here would re-order the same
    // rows a second time, and would do it before the rows are narrowed.
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT series_fingerprint, timestamp, value FROM {table}"
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
