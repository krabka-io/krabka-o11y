use std::{collections::BTreeMap, sync::Arc};

use krabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};

use super::{
    PromqlEngine,
    annotations::emit_warning,
    merge_by_fingerprint::merge_by_fingerprint,
    row_cache::{
        FloatRow, FloatWindow, HistogramRow, RANGE_SCAN_CACHE, collect_float_rows,
        collect_histogram_rows, matchers_cache_key,
    },
    samples_per_query_exceeded, series_per_query_exceeded,
};
use crate::{
    ScanResult,
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
    async fn labels_by_fingerprint(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<BTreeMap<SeriesFingerprint, Arc<Labels>>>> {
        // Range queries resolve the same selector's series at every step. Labels
        // are window-independent, so cache the union-window resolution once per
        // matcher set and reuse it across steps (see RANGE_SCAN_CACHE). Requests
        // outside the pre-scanned union fall back to a direct resolution.
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.labels.get(&key).cloned()
                };
                let resolved = if let Some(map) = cached {
                    map
                } else {
                    let map = Arc::new(
                        self.labels_by_fingerprint_uncached(
                            tenant,
                            matchers,
                            full_start_ms,
                            full_end_ms,
                        )
                        .await?,
                    );
                    cache
                        .lock()
                        .expect("range scan cache poisoned")
                        .labels
                        .insert(key, Arc::clone(&map));
                    map
                };
                return Ok(resolved);
            }
        }
        Ok(Arc::new(
            self.labels_by_fingerprint_uncached(tenant, matchers, start_ms, end_ms)
                .await?,
        ))
    }

    async fn labels_by_fingerprint_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeMap<SeriesFingerprint, Arc<Labels>>> {
        Ok(self
            .store
            .series(tenant, matchers, start_ms, end_ms)
            .await?
            .into_iter()
            .map(|labels| (labels.fingerprint(), Arc::new(labels)))
            .collect())
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

    /// The indexed float rows covering `[start_ms, end_ms]` for one matcher set.
    ///
    /// Inside a range query (see `RANGE_SCAN_CACHE`) this is the union-window
    /// index, shared across every step: overlapping per-step scans are served
    /// from it by binary search instead of re-scanning the store. A request that
    /// falls outside the pre-scanned union (offset/`@`-modifier, or a `[range]`
    /// longer than the lookback) bypasses the cache and scans directly, so
    /// results are identical — only redundant re-scans are eliminated. The
    /// returned window may therefore cover more than `[start_ms, end_ms]`, and
    /// every caller narrows it before use.
    async fn float_window(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<FloatWindow>> {
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.floats.get(&key).cloned()
                };
                if let Some(window) = cached {
                    return Ok(window);
                }
                let window = Arc::new(FloatWindow::new(
                    self.scan_float_rows_uncached(tenant, matchers, full_start_ms, full_end_ms)
                        .await?,
                ));
                cache
                    .lock()
                    .expect("range scan cache poisoned")
                    .floats
                    .insert(key, Arc::clone(&window));
                return Ok(window);
            }
        }
        Ok(Arc::new(FloatWindow::new(
            self.scan_float_rows_uncached(tenant, matchers, start_ms, end_ms)
                .await?,
        )))
    }

    async fn scan_float_rows_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FloatRow>> {
        let scan = self.store.scan(tenant, matchers, start_ms, end_ms).await?;
        emit_scan_warnings(&scan);
        let Some(table) = scan.float_table.clone() else {
            return Ok(Vec::new());
        };
        collect_float_rows(scan, &table, self.opts.max_samples).await
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
        Ok(out)
    }

    /// Whether any matcher set has a histogram sample in `(after_ms, through_ms]`.
    ///
    /// The two `*_has_histogram_series` gates ask only this. Materializing the
    /// whole window to answer it would deep-copy a `NativeHistogram` per row per
    /// step, so stop at the first hit instead.
    pub(super) async fn histogram_rows_present_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        after_ms: i64,
        through_ms: i64,
    ) -> Result<bool> {
        for matchers in matcher_sets {
            if self
                .cached_histogram_rows(tenant, matchers, after_ms, through_ms)
                .await?
                .iter()
                .any(|row| row.ts_ms > after_ms && row.ts_ms <= through_ms)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The histogram rows covering `[start_ms, end_ms]` for one matcher set,
    /// borrowed from the range cache when it holds them.
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
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.histograms.get(&key).cloned()
                };
                if let Some(rows) = cached {
                    return Ok(rows);
                }
                let rows = Arc::new(
                    self.scan_histogram_rows_uncached(tenant, matchers, full_start_ms, full_end_ms)
                        .await?,
                );
                cache
                    .lock()
                    .expect("range scan cache poisoned")
                    .histograms
                    .insert(key, Arc::clone(&rows));
                return Ok(rows);
            }
        }
        Ok(Arc::new(
            self.scan_histogram_rows_uncached(tenant, matchers, start_ms, end_ms)
                .await?,
        ))
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

    async fn scan_histogram_rows_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        let scan = self.store.scan(tenant, matchers, start_ms, end_ms).await?;
        emit_scan_warnings(&scan);
        let Some(table) = scan.histogram_table.clone() else {
            return Ok(Vec::new());
        };
        collect_histogram_rows(scan, &table, self.opts.max_samples).await
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
        Ok(out)
    }
}
