use super::{
    FLOAT_ROW_COLUMNS, FloatRow, FloatRowColumns, Result, SessionContext,
    samples_per_query_exceeded,
};

pub(crate) async fn collect_float_rows(
    ctx: &SessionContext,
    table: &str,
    max_samples: usize,
) -> Result<Vec<FloatRow>> {
    // No `ORDER BY`. The store bounds the scan to the requested series and
    // window, and the only caller wraps the result in a `FloatWindow`, which
    // establishes `(series_fingerprint, timestamp)` order itself — and skips the
    // sort outright when the rows already arrive that way, which they do when a
    // single block answers the scan. A global sort here would re-order the same
    // rows a second time, and would do it before the rows are narrowed.
    let dataframe = ctx
        .sql(&format!("SELECT {FLOAT_ROW_COLUMNS} FROM {table}"))
        .await?;
    let batches = dataframe.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        let columns = FloatRowColumns::from_batch(&batch);
        for row in 0..batch.num_rows() {
            // The cap trips on the row that would take the count past it, so the
            // count this row would produce is what the tenant is told.
            if rows.len() >= max_samples {
                return Err(samples_per_query_exceeded(
                    max_samples,
                    rows.len().saturating_add(1),
                ));
            }
            rows.push(columns.row(row));
        }
    }
    Ok(rows)
}
