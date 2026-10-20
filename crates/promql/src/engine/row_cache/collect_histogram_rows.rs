use std::cmp::Ordering;

use super::{
    HistogramRow, PromqlError, Result, SessionContext, decode_native_histograms,
    samples_per_query_exceeded,
};

pub(crate) async fn collect_histogram_rows(
    ctx: &SessionContext,
    table: &str,
    max_samples: usize,
) -> Result<Vec<HistogramRow>> {
    // No `ORDER BY`: the rows are re-ordered below, once, after the store has
    // narrowed them, rather than by a global sort over everything the scan
    // decoded. The sort is skipped when they already arrive ordered.
    let dataframe = ctx.sql(&format!("SELECT * FROM {table}")).await?;
    let batches = dataframe.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        let decoded = decode_native_histograms(&batch)
            .map_err(|error| PromqlError::Store(error.to_string()))?;
        for row in decoded {
            // The cap trips on the row that would take the count past it, so the
            // count this row would produce is what the tenant is told.
            if rows.len() >= max_samples {
                return Err(samples_per_query_exceeded(
                    max_samples,
                    rows.len().saturating_add(1),
                ));
            }
            rows.push(HistogramRow {
                fp: row.fingerprint,
                ts_ms: row.timestamp_ms,
                hist: row.hist,
            });
        }
    }
    if rows
        .windows(2)
        .any(|pair| compare_rows(&pair[0], &pair[1]) == Ordering::Greater)
    {
        rows.sort_by(compare_rows);
    }
    Ok(rows)
}

fn compare_rows(left: &HistogramRow, right: &HistogramRow) -> Ordering {
    left.fp.cmp(&right.fp).then(left.ts_ms.cmp(&right.ts_ms))
}
