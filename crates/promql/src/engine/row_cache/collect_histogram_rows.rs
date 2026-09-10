use super::{HistogramRow, PromqlError, Result, ScanResult, decode_native_histograms};

pub(crate) async fn collect_histogram_rows(
    scan: ScanResult,
    table: &str,
    max_samples: usize,
) -> Result<Vec<HistogramRow>> {
    // No `ORDER BY`: the rows are re-ordered below, once, after the store has
    // narrowed them, rather than by a global sort over everything the scan
    // decoded. The sort is skipped when they already arrive ordered.
    let dataframe = scan.ctx.sql(&format!("SELECT * FROM {table}")).await?;
    let batches = dataframe.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        let decoded = decode_native_histograms(&batch)
            .map_err(|error| PromqlError::Store(error.to_string()))?;
        for (fp, ts_ms, hist) in decoded {
            if rows.len() >= max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={max_samples}"
                )));
            }
            rows.push(HistogramRow { fp, ts_ms, hist });
        }
    }
    if !rows.is_sorted_by_key(|row| (row.fp, row.ts_ms)) {
        rows.sort_by_key(|row| (row.fp, row.ts_ms));
    }
    Ok(rows)
}
