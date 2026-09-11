use super::{
    HistogramRow, PromqlError, Result, ScanResult, decode_native_histograms,
    samples_per_query_exceeded,
};

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
            // The cap trips on the row that would take the count past it, so the
            // count this row would produce is what the tenant is told.
            if rows.len() >= max_samples {
                return Err(samples_per_query_exceeded(
                    max_samples,
                    rows.len().saturating_add(1),
                ));
            }
            rows.push(HistogramRow { fp, ts_ms, hist });
        }
    }
    if !rows.is_sorted_by_key(|row| (row.fp, row.ts_ms)) {
        rows.sort_by_key(|row| (row.fp, row.ts_ms));
    }
    Ok(rows)
}
