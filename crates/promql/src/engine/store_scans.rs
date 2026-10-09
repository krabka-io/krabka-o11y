use std::{collections::BTreeMap, sync::Arc};

use krabka_blockstore::SeriesFingerprint;

use super::{
    PromqlEngine,
    annotations::emit_warning,
    merge_by_fingerprint::merge_by_fingerprint,
    record_queryable_samples,
    row_cache::{
        FloatRow, FloatWindow, HistogramRow, RANGE_SCAN_CACHE, ScannedRows, through_scan_cache,
    },
    samples_per_query_exceeded, series_per_query_exceeded,
};
use crate::{
    PromqlLabels as Labels, PromqlMatcher as LabelMatcher, ScanResult,
    error::Result,
    extension::is_stale_nan,
    planner::{LabeledSeries, TimedValue},
    store::MetricStore,
};

/// Raises one `PromQL` warning for each block the scan answered without.
///
/// The warnings leave through the same annotation sink as every other engine
/// warning, so a caller that reads the annotations of a query sees that the
/// result is short a block. The warning is raised before the table is read,
/// because a scan whose only candidate block was deleted registers no table
/// and still has to report the block.
fn emit_scan_warnings(scan: &ScanResult) {
    for warning in &scan.warnings {
        emit_warning(warning.clone());
    }
}

impl<S: MetricStore> PromqlEngine<S> {
    pub(super) async fn latest_labeled_series(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        after_ms: i64,
        through_ms: i64,
    ) -> Result<Option<Vec<LabeledSeries>>> {
        if after_ms >= through_ms {
            return Ok(None);
        }
        let allowed = RANGE_SCAN_CACHE
            .try_with(|cache| {
                cache
                    .lock()
                    .expect("range scan cache poisoned")
                    .allow_latest_float_samples
            })
            .unwrap_or(false);
        if !allowed {
            return Ok(None);
        }
        let [matchers] = matcher_sets else {
            return Ok(None);
        };
        let Some(scan) = self
            .store
            .try_latest_float_scan(
                tenant,
                matchers,
                after_ms,
                after_ms.saturating_add(1),
                through_ms,
                self.opts.max_samples,
            )
            .await?
        else {
            return Ok(None);
        };
        let rows = scan.samples;
        if rows.len() > self.opts.max_samples {
            return Err(samples_per_query_exceeded(
                self.opts.max_samples,
                rows.len(),
            ));
        }
        let labels = scan.labels;
        let max_fetched_series = self.opts.max_fetched_series;
        if max_fetched_series != 0 && labels.len() > max_fetched_series {
            return Err(series_per_query_exceeded(max_fetched_series, labels.len()));
        }
        record_queryable_samples(rows.len());
        Ok(Some(
            rows.into_iter()
                .filter_map(|(fp, ts_ms, value, start_timestamp_ms)| {
                    Some(LabeledSeries {
                        fp,
                        labels: Arc::clone(labels.get(&fp)?),
                        samples: vec![TimedValue {
                            ts_ms,
                            value,
                            start_timestamp_ms,
                        }],
                    })
                })
                .collect(),
        ))
    }

    /// The series of one matcher set over `[start_ms, end_ms]`, by fingerprint.
    ///
    /// A range query resolves the same selector's series at every step. Labels
    /// are window-independent, so a range query keeps one resolution of its
    /// union window per matcher set, and every step reads it (see
    /// `RANGE_SCAN_CACHE`). An instant query does not keep the resolution: a
    /// wider window can hold more series, and the series limit of each
    /// selector counts only its own window.
    async fn labels_by_fingerprint(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<BTreeMap<SeriesFingerprint, Arc<Labels>>>> {
        through_scan_cache(
            |cache| &mut cache.labels,
            true,
            matchers,
            start_ms,
            end_ms,
            |start_ms, end_ms| {
                self.labels_by_fingerprint_uncached(tenant, matchers, start_ms, end_ms)
            },
        )
        .await
    }

    async fn labels_by_fingerprint_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeMap<SeriesFingerprint, Arc<Labels>>> {
        self.store
            .series_shared_by_fingerprint(tenant, matchers, start_ms, end_ms)
            .await
    }

    /// Resolves every matcher set's series to one `fingerprint -> labels` map.
    ///
    /// The label sets are shared rather than copied: a range query attaches the
    /// same label set to every sample of every step, so a per-sample deep copy
    /// of a `BTreeMap<String, String>` is the single largest allocation source
    /// on the step loop. The single-matcher-set case, which is every selector
    /// without an `or`, hands back the cached map itself.
    pub(super) async fn labels_by_fingerprint_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<BTreeMap<SeriesFingerprint, Arc<Labels>>>> {
        let resolved = if let [matchers] = matcher_sets {
            self.labels_by_fingerprint(tenant, matchers, start_ms, end_ms)
                .await?
        } else {
            let mut out = BTreeMap::new();
            for matchers in matcher_sets {
                out.extend(
                    self.labels_by_fingerprint(tenant, matchers, start_ms, end_ms)
                        .await?
                        .iter()
                        .map(|(fp, labels)| (*fp, Arc::clone(labels))),
                );
            }
            Arc::new(out)
        };
        let max_fetched_series = self.opts.max_fetched_series;
        if max_fetched_series != 0 && resolved.len() > max_fetched_series {
            return Err(series_per_query_exceeded(
                max_fetched_series,
                resolved.len(),
            ));
        }
        Ok(resolved)
    }

    /// The tables of one store scan that covers `[start_ms, end_ms]` for one
    /// matcher set.
    ///
    /// Inside a query scope (see `RANGE_SCAN_CACHE`) the scan is shared: a
    /// range query scans its union window once for every step, and an instant
    /// query scans a selector once for its histogram probe and its floats. A
    /// request that the cache does not keep scans the store directly, so
    /// results are identical and only redundant scans are removed. The scan may
    /// therefore cover more than `[start_ms, end_ms]`, and every caller narrows
    /// its rows before use.
    async fn scanned_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<ScannedRows>> {
        through_scan_cache(
            |cache| &mut cache.rows,
            false,
            matchers,
            start_ms,
            end_ms,
            |start_ms, end_ms| async move {
                let scan = self.store.scan(tenant, matchers, start_ms, end_ms).await?;
                emit_scan_warnings(&scan);
                Ok(ScannedRows::new(scan))
            },
        )
        .await
    }

    /// The indexed float rows covering `[start_ms, end_ms]` for one matcher set.
    ///
    /// The rows may cover more than `[start_ms, end_ms]`. See
    /// [`Self::scanned_rows`].
    pub(super) async fn float_window(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<FloatWindow>> {
        self.scanned_rows(tenant, matchers, start_ms, end_ms)
            .await?
            .floats(self.opts.max_samples)
            .await
    }

    /// Last non-stale sample per series and the complete in-window row count.
    pub(super) async fn last_float_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(usize, Vec<FloatRow>)> {
        let scan = self
            .scanned_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        if self.store.float_samples_are_unique()
            && let Some(last) = scan
                .try_last_floats(self.opts.max_samples, start_ms, end_ms)
                .await?
        {
            return Ok(last);
        }
        let window = scan.floats(self.opts.max_samples).await?;
        let mut total = 0;
        let rows = window
            .series(start_ms, end_ms)
            .filter_map(|(_, rows)| {
                total += rows.len();
                rows.iter()
                    .rev()
                    .find(|row| !is_stale_nan(row.value))
                    .copied()
            })
            .collect();
        Ok((total, rows))
    }

    /// Every matcher set's float rows over `[start_ms, end_ms]`, flattened.
    ///
    /// Rows come back in `(fingerprint, timestamp)` order within each matcher
    /// set, which is the order the operator leaves need.
    pub(super) async fn scan_float_row_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FloatRow>> {
        let mut out = Vec::new();
        for matchers in matcher_sets {
            let window = self
                .float_window(tenant, matchers, start_ms, end_ms)
                .await?;
            out.extend(window.rows_between(start_ms, end_ms));
            if out.len() > self.opts.max_samples {
                return Err(samples_per_query_exceeded(self.opts.max_samples, out.len()));
            }
        }
        record_queryable_samples(out.len());
        Ok(out)
    }

    /// Every matcher set's float rows over the half-open window
    /// `(after_ms, through_ms]`, grouped into one entry per series with its
    /// label set attached.
    ///
    /// This is what the three operator leaves consume. Grouping is what keeps
    /// the step loop cheap: the label set is attached once per series rather
    /// than copied onto every sample, and the samples of a series arrive as one
    /// time-ordered run, so the leaf needs no sort. `drop_stale_nan` follows the
    /// selector kind — a matrix selector drops the markers, an instant selector
    /// keeps them so `InstantManipulate` can suppress the series.
    pub(super) async fn labeled_series_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        after_ms: i64,
        through_ms: i64,
        drop_stale_nan: bool,
    ) -> Result<Vec<LabeledSeries>> {
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, matcher_sets, after_ms, through_ms)
            .await?;
        // The window is left-open, and timestamps are whole milliseconds, so the
        // first included instant is one millisecond after the bound.
        let from_ms = after_ms.saturating_add(1);

        let mut out: Vec<LabeledSeries> = Vec::new();
        let mut total = 0_usize;
        for matchers in matcher_sets {
            let window = self
                .float_window(tenant, matchers, from_ms, through_ms)
                .await?;
            for (fp, rows) in window.series(from_ms, through_ms) {
                // The cap counts what the window holds, not what survives the
                // label lookup and the staleness filter, which is what a scan of
                // the same window would have returned.
                total += rows.len();
                if total > self.opts.max_samples {
                    return Err(samples_per_query_exceeded(self.opts.max_samples, total));
                }
                let Some(labels) = labels_by_fp.get(&fp) else {
                    continue;
                };
                let samples = rows
                    .iter()
                    .filter(|row| !(drop_stale_nan && is_stale_nan(row.value)))
                    .map(|row| TimedValue {
                        ts_ms: row.ts_ms,
                        value: row.value,
                        start_timestamp_ms: row.start_timestamp_ms,
                    })
                    .collect::<Vec<_>>();
                if samples.is_empty() {
                    continue;
                }
                out.push(LabeledSeries {
                    fp,
                    labels: Arc::clone(labels),
                    samples,
                });
            }
        }
        // A selector with `or` branches can match the same series from more than
        // one branch, and each branch contributes its own run. Fold them, so
        // `SeriesDivide` still sees one run per series.
        if matcher_sets.len() > 1 {
            out = merge_by_fingerprint(out);
        }
        record_queryable_samples(total);
        Ok(out)
    }

    /// Whether any matcher set has a histogram sample in `(after_ms, through_ms]`.
    ///
    /// The two `*_has_histogram_series` gates ask only this. Materializing the
    /// whole window to answer it would deep-copy a `NativeHistogram` per row per
    /// step, so stop at the first hit instead.
    ///
    /// The store can often tell from its index alone that no histogram matches,
    /// for example when it holds no histogram blocks. Then the probe makes no
    /// scan.
    pub(super) async fn histogram_rows_present_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        after_ms: i64,
        through_ms: i64,
    ) -> Result<bool> {
        // The window is left-open, and timestamps are whole milliseconds, so the
        // first included instant is one millisecond after the bound. This is
        // the window of the float read that follows on the operator path, so
        // the probe and that read share one scan, and the scan holds no row
        // that the float read did not already hold.
        let from_ms = after_ms.saturating_add(1);
        for matchers in matcher_sets {
            if !self
                .store
                .may_have_histograms(tenant, matchers, from_ms, through_ms)
                .await?
            {
                continue;
            }
            if self
                .cached_histogram_rows(tenant, matchers, from_ms, through_ms)
                .await?
                .iter()
                .any(|row| row.ts_ms >= from_ms && row.ts_ms <= through_ms)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The histogram rows covering `[start_ms, end_ms]` for one matcher set.
    ///
    /// Like [`Self::float_window`], the returned rows may cover more than
    /// `[start_ms, end_ms]`, so every caller narrows them.
    async fn cached_histogram_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<Vec<HistogramRow>>> {
        self.scanned_rows(tenant, matchers, start_ms, end_ms)
            .await?
            .histograms(self.opts.max_samples)
            .await
    }

    async fn scan_histogram_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        Ok(self
            .cached_histogram_rows(tenant, matchers, start_ms, end_ms)
            .await?
            .iter()
            .filter(|row| row.ts_ms >= start_ms && row.ts_ms <= end_ms)
            .cloned()
            .collect())
    }

    pub(super) async fn scan_histogram_row_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        let mut out = Vec::new();
        for matchers in matcher_sets {
            out.extend(
                self.scan_histogram_rows(tenant, matchers, start_ms, end_ms)
                    .await?,
            );
            if out.len() > self.opts.max_samples {
                return Err(samples_per_query_exceeded(self.opts.max_samples, out.len()));
            }
        }
        record_queryable_samples(out.len());
        Ok(out)
    }
}
