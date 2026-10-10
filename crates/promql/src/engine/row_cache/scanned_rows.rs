use std::collections::HashMap;

use futures::TryStreamExt as _;
use tokio::sync::OnceCell;

use super::{
    Arc, FLOAT_ROW_COLUMNS, FloatRow, FloatRowColumns, FloatWindow, HistogramRow, Result,
    ScanResult, SessionContext, collect_float_rows, collect_histogram_rows,
    samples_per_query_exceeded,
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

    /// Last non-stale floats from a unique scan, without a full row index.
    ///
    /// An existing float window stays on its indexed path. The table remains
    /// registered so another expression can still read the complete scan.
    pub(crate) async fn try_last_floats(
        &self,
        max_samples: usize,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Option<(usize, Vec<FloatRow>)>> {
        if self.floats.get().is_some() {
            return Ok(None);
        }
        let Some(table) = &self.float_table else {
            return Ok(Some((0, Vec::new())));
        };
        let dataframe = self
            .ctx
            .sql(&format!("SELECT {FLOAT_ROW_COLUMNS} FROM {table}"))
            .await?;
        let mut stream = dataframe.execute_stream().await?;
        let mut last = HashMap::<_, FloatRow, ahash::RandomState>::default();
        let mut scanned = 0_usize;
        let mut total = 0;
        while let Some(batch) = stream.try_next().await? {
            // The ordinary collector reads all batches before checking the
            // row cap. Drain the stream after the cap to retain its errors.
            if scanned > max_samples {
                continue;
            }
            let columns = FloatRowColumns::from_batch(&batch);
            for row in 0..batch.num_rows() {
                scanned = scanned.saturating_add(1);
                if scanned > max_samples {
                    break;
                }
                let float_row = columns.row(row);
                if float_row.ts_ms < from_ms || float_row.ts_ms > to_ms {
                    continue;
                }
                total += 1;
                if crate::extension::is_stale_nan(float_row.value) {
                    continue;
                }
                if last
                    .get(&float_row.fp)
                    .is_none_or(|previous| float_row.ts_ms > previous.ts_ms)
                {
                    last.insert(float_row.fp, float_row);
                }
            }
        }
        if scanned > max_samples {
            return Err(samples_per_query_exceeded(max_samples, scanned));
        }
        let mut rows = last.into_values().collect::<Vec<_>>();
        rows.sort_unstable_by_key(|row| row.fp);
        Ok(Some((total, rows)))
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
