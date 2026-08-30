use super::{ScanResult, Result, HistogramRow, decode_native_histograms, PromqlError};

pub(crate) async fn collect_histogram_rows(
    scan: ScanResult,
    table: &str,
    max_samples: usize,
) -> Result<Vec<HistogramRow>> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT * FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await?;
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
    Ok(rows)
}
