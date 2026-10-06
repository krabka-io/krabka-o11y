use tokio::sync::OnceCell;

use super::{
    Arc, FloatWindow, HistogramRow, Result, ScanResult, SessionContext, collect_float_rows,
    collect_histogram_rows,
};

/// The tables of one store scan, read into rows on first use.
///
/// The planner asks a selector for its histogram rows and then for its float
/// rows over the same window. Both questions read the same scan here, so one
/// store scan answers both. Each table is read only when a caller asks for it,
/// so a caller that wants only floats never reads the histogram table. A table
/// is deregistered once it is read, so the cache does not hold the scanned
/// batches of an in-memory store next to the rows read from them.
pub(crate) struct ScannedRows {
    ctx: SessionContext,
    float_table: Option<String>,
    histogram_table: Option<String>,
    floats: OnceCell<Arc<FloatWindow>>,
    histograms: OnceCell<Arc<Vec<HistogramRow>>>,
}

impl ScannedRows {
    pub(crate) fn new(scan: ScanResult) -> Self {
        Self {
            ctx: scan.ctx,
            float_table: scan.float_table,
            histogram_table: scan.histogram_table,
            floats: OnceCell::new(),
            histograms: OnceCell::new(),
        }
    }

    /// The float rows of the scan, indexed by series.
    pub(crate) async fn floats(&self, max_samples: usize) -> Result<Arc<FloatWindow>> {
        self.floats
            .get_or_try_init(|| async {
                let rows = match &self.float_table {
                    Some(table) => {
                        let rows = collect_float_rows(&self.ctx, table, max_samples).await?;
                        self.ctx.deregister_table(table.as_str())?;
                        rows
                    }
                    None => Vec::new(),
                };
                Ok(Arc::new(FloatWindow::new(rows)))
            })
            .await
            .map(Arc::clone)
    }

    /// The histogram rows of the scan, in `(fingerprint, timestamp)` order.
    pub(crate) async fn histograms(&self, max_samples: usize) -> Result<Arc<Vec<HistogramRow>>> {
        self.histograms
            .get_or_try_init(|| async {
                let mut rows = match &self.histogram_table {
                    Some(table) => {
                        let rows = collect_histogram_rows(&self.ctx, table, max_samples).await?;
                        self.ctx.deregister_table(table.as_str())?;
                        rows
                    }
                    None => Vec::new(),
                };
                rows.sort_by_key(|row| (row.fp, row.ts_ms));
                rows.dedup_by(|later, earlier| {
                    if (later.fp, later.ts_ms) == (earlier.fp, earlier.ts_ms) {
                        earlier.clone_from(later);
                        true
                    } else {
                        false
                    }
                });
                Ok(Arc::new(rows))
            })
            .await
            .map(Arc::clone)
    }
}
