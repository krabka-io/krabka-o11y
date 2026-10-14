use std::{
    collections::{BTreeMap, BTreeSet, HashMap, hash_map::Entry},
    sync::Arc,
};

use futures::TryStreamExt;
use krabka_metrics::{FloatSampleRow, decode_float_samples, float_sample_schema};

use super::MergedMetricStore;
use crate::{
    LatestFloatScan, MetricBlockStore, MetricStore, PromqlError, PromqlLabels as Labels,
    PromqlMatcher as LabelMatcher, WalHead,
    in_memory::{prepare_matchers, row_matches},
};

#[derive(Default)]
struct LatestSeries {
    sample: Option<FloatSampleRow>,
    labels: Option<Arc<Labels>>,
    matching_labels: Option<Arc<Labels>>,
}

struct ColdCandidates {
    fingerprints: BTreeSet<u64>,
    keys: Vec<String>,
    blocks: Vec<krabka_blockstore::BlockMeta>,
    labels: BTreeMap<u64, Labels>,
}

struct LatestSeriesEntry {
    fingerprint: u64,
    series: LatestSeries,
}

impl MergedMetricStore<MetricBlockStore, WalHead> {
    /// Return latest float samples when a conservative full-window count fits
    /// the query limit. Skip cold blocks dominated by the captured hot head,
    /// and stream the remaining blocks. Uncertain metadata, cold-only scans
    /// and ambiguous cold ties retain the ordinary complete scan.
    ///
    /// # Errors
    /// Returns an error when label matchers are invalid.
    pub async fn try_latest_float_samples(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
        max_samples: usize,
    ) -> Result<Option<Vec<FloatSampleRow>>, PromqlError> {
        Ok(self
            .try_latest_float_scan(tenant, matchers, start_ms, start_ms, end_ms, max_samples)
            .await?
            .map(|scan| scan.samples))
    }

    /// Latest samples and cold-first labels from one captured hot head.
    ///
    /// The label window can include the excluded sample lookback boundary.
    /// Histogram labels at that boundary still contribute to the series limit.
    ///
    /// # Errors
    /// Returns an error when label matchers or cold metadata are invalid.
    #[allow(clippy::too_many_lines)]
    pub async fn try_latest_float_scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_start_ms: i64,
        start_ms: i64,
        end_ms: i64,
        max_samples: usize,
    ) -> Result<Option<LatestFloatScan>, PromqlError> {
        if label_start_ms > start_ms {
            return Ok(None);
        }
        let hot = self.hot.snapshot();
        if self
            .cold
            .may_have_histograms(tenant, matchers, start_ms, end_ms)
            .await?
            || hot
                .may_have_histograms(tenant, matchers, start_ms, end_ms)
                .await?
        {
            return Ok(None);
        }
        let prepared = prepare_matchers(matchers)?;
        let index = self.cold.floats.index();
        // Keep resolution errors pending until the original cold-read point.
        // An exact hot count can still require the ordinary scan before then.
        let cold_candidates =
            self.cold
                .matching_series_by_fp(tenant, matchers)
                .map(|cold_labels| {
                    let fps = cold_labels.keys().copied().collect::<BTreeSet<_>>();
                    let keys = index.candidate_blocks(tenant, &fps, start_ms, end_ms);
                    let blocks = index.all_blocks(tenant);
                    ColdCandidates {
                        fingerprints: fps,
                        keys,
                        blocks,
                        labels: cold_labels,
                    }
                });
        let mut latest = HashMap::<u64, LatestSeries, ahash::RandomState>::default();
        let mut upper_count = 0_usize;
        if let Some(rows) = hot.floats.get(tenant) {
            let summary = hot
                .float_head_summaries
                .get(tenant)
                .and_then(|summary| {
                    summary.matching_series(rows.len(), &prepared, label_start_ms, start_ms, end_ms)
                })
                .filter(|summary| {
                    // A loose bound must use the exact hot loop, not the full
                    // scan: its boundary and duplicate limit behavior differs.
                    cold_candidates.as_ref().is_ok_and(|candidates| {
                        candidates
                            .blocks
                            .iter()
                            .filter(|block| candidates.keys.contains(&block.object_key))
                            .fold(summary.total_rows, |count, block| {
                                count.saturating_add(block.row_count)
                            })
                            <= max_samples
                    })
                });
            if let Some(summary) = summary {
                upper_count = summary.total_rows;
                for entry in summary.series {
                    latest.insert(
                        entry.latest.fingerprint,
                        LatestSeries {
                            sample: (entry.latest.timestamp_ms >= start_ms).then_some(entry.latest),
                            labels: Some(Arc::clone(&entry.labels)),
                            ..LatestSeries::default()
                        },
                    );
                }
            } else {
                for row in rows.iter() {
                    if row.ts_ms < label_start_ms || row.ts_ms > end_ms {
                        continue;
                    }
                    let series = match latest.entry(row.fp) {
                        Entry::Occupied(entry) => {
                            let series = entry.into_mut();
                            // A shared immutable label set has the same matcher
                            // result at every in-window row of this fingerprint.
                            if !series
                                .matching_labels
                                .as_ref()
                                .is_some_and(|labels| Arc::ptr_eq(labels, &row.labels))
                            {
                                if !row_matches(
                                    row.fp,
                                    &row.labels,
                                    row.ts_ms,
                                    &prepared,
                                    label_start_ms,
                                    end_ms,
                                ) {
                                    continue;
                                }
                                series.matching_labels = Some(Arc::clone(&row.labels));
                            }
                            series
                        }
                        Entry::Vacant(entry) => {
                            if !row_matches(
                                row.fp,
                                &row.labels,
                                row.ts_ms,
                                &prepared,
                                label_start_ms,
                                end_ms,
                            ) {
                                continue;
                            }
                            entry.insert(LatestSeries {
                                sample: None,
                                labels: Some(Arc::clone(&row.labels)),
                                matching_labels: Some(Arc::clone(&row.labels)),
                            })
                        }
                    };
                    if row.ts_ms < start_ms {
                        continue;
                    }
                    upper_count += 1;
                    if upper_count > max_samples {
                        return Ok(None);
                    }
                    // Keep the first hot row at equal timestamps, including stale
                    // markers and creation timestamps.
                    if series
                        .sample
                        .is_none_or(|previous| row.ts_ms > previous.timestamp_ms)
                    {
                        series.sample = Some(FloatSampleRow {
                            fingerprint: row.fp,
                            timestamp_ms: row.ts_ms,
                            value: row.value,
                            start_timestamp_ms: row.start_timestamp_ms,
                        });
                    }
                }
            }
        }
        if label_start_ms < start_ms
            && let Some(rows) = hot.hists.get(tenant)
        {
            for row in rows.iter() {
                if row.ts_ms >= start_ms || latest.contains_key(&row.fp) {
                    continue;
                }
                if row_matches(
                    row.fp,
                    &row.labels,
                    row.ts_ms,
                    &prepared,
                    label_start_ms,
                    end_ms,
                ) {
                    latest.insert(
                        row.fp,
                        LatestSeries {
                            labels: Some(Arc::clone(&row.labels)),
                            ..LatestSeries::default()
                        },
                    );
                }
            }
        }
        // Selected values and labels retain the captured head's result.
        // Release its rows before the cold read can wait on object storage.
        drop(hot);
        let candidates = cold_candidates?;
        let mut covered_blocks = 0;
        let mut uncovered = Vec::new();
        for block in candidates.blocks {
            if !candidates.keys.contains(&block.object_key) {
                continue;
            }
            covered_blocks += 1;
            upper_count = upper_count.saturating_add(block.row_count);
            if upper_count > max_samples || block.fingerprints.is_empty() {
                return Ok(None);
            }
            if block
                .fingerprints
                .iter()
                .filter(|fp| candidates.fingerprints.contains(fp))
                .any(|fp| {
                    latest
                        .get(fp)
                        .and_then(|series| series.sample)
                        .is_none_or(|row| row.timestamp_ms < block.max_ts)
                })
            {
                uncovered.push(block.object_key.clone());
            }
        }
        if covered_blocks != candidates.keys.len()
            || (!uncovered.is_empty() && latest.values().all(|series| series.sample.is_none()))
        {
            return Ok(None);
        }
        // A WAL-head reader can briefly trail a newly published block.
        // Read only blocks that can change a latest value, using the same
        // read caps, footer cache and streaming scan as the ordinary path.
        if !uncovered.is_empty()
            && !self
                .try_merge_latest_cold_samples(
                    &uncovered,
                    &candidates.fingerprints,
                    start_ms,
                    end_ms,
                    &mut latest,
                )
                .await?
        {
            return Ok(None);
        }
        let mut series = latest
            .into_iter()
            .map(|entry| LatestSeriesEntry {
                fingerprint: entry.0,
                series: entry.1,
            })
            .collect::<Vec<_>>();
        series.sort_unstable_by_key(|entry| entry.fingerprint);
        let mut labels = BTreeMap::new();
        for entry in candidates.labels {
            labels.insert(entry.0, Arc::new(entry.1));
        }
        let mut samples = Vec::new();
        for entry in series {
            if let Some(hot) = entry.series.labels {
                labels.entry(hot.fingerprint()).or_insert(hot);
            }
            if let Some(sample) = entry.series.sample {
                samples.push(sample);
            }
        }
        Ok(Some(LatestFloatScan { samples, labels }))
    }

    async fn try_merge_latest_cold_samples(
        &self,
        keys: &[String],
        fps: &BTreeSet<u64>,
        start_ms: i64,
        end_ms: i64,
        latest: &mut HashMap<u64, LatestSeries, ahash::RandomState>,
    ) -> Result<bool, PromqlError> {
        let scan = self
            .cold
            .floats
            .scan_block_keys_skipping_unreadable(keys, float_sample_schema())
            .await
            .map_err(|error| PromqlError::Store(error.to_string()))?;
        if !scan.report.skipped.is_empty() {
            // The full scan retains the established missing-block warnings
            // and corrupt-block errors. Do not lose them in a row-only result.
            return Ok(false);
        }
        let mut stream = scan.ctx.table(&scan.table).await?.execute_stream().await?;
        let mut cold_latest = HashMap::<u64, FloatSampleRow>::new();
        while let Some(batch) = stream.try_next().await? {
            let Ok(rows) = decode_float_samples(&batch) else {
                return Ok(false);
            };
            for row in rows {
                if !fps.contains(&row.fingerprint)
                    || row.timestamp_ms < start_ms
                    || row.timestamp_ms > end_ms
                {
                    continue;
                }
                if let Some(previous) = cold_latest.get_mut(&row.fingerprint) {
                    if previous.timestamp_ms == row.timestamp_ms
                        && (previous.value.to_bits() != row.value.to_bits()
                            || previous.start_timestamp_ms != row.start_timestamp_ms)
                    {
                        // Equal-time conflicting cold values retain the
                        // full merge's tie behavior instead of choosing anew.
                        return Ok(false);
                    }
                    if row.timestamp_ms > previous.timestamp_ms {
                        *previous = row;
                    }
                } else {
                    cold_latest.insert(row.fingerprint, row);
                }
            }
        }
        for entry in cold_latest {
            let series = latest.entry(entry.0).or_default();
            // Hot wins at equal timestamps, just as in a full scan.
            if series
                .sample
                .is_none_or(|previous| entry.1.timestamp_ms > previous.timestamp_ms)
            {
                series.sample = Some(entry.1);
            }
        }
        Ok(true)
    }
}
