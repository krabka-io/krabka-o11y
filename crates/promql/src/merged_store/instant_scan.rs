use std::collections::HashMap;

use krabka_blockstore::LabelMatcher;
use krabka_metrics::FloatSampleRow;

use super::MergedMetricStore;
use crate::{
    MetricBlockStore, MetricStore, PromqlError, WalHead,
    in_memory::{prepare_matchers, row_matches},
};

impl MergedMetricStore<MetricBlockStore, WalHead> {
    /// Return latest hot samples only when every relevant cold sample is
    /// dominated and a conservative full-window count fits the query limit.
    /// Otherwise the caller uses the ordinary complete scan.
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
        let mut latest = HashMap::<u64, FloatSampleRow>::new();
        let mut upper_count = 0_usize;
        if let Some(rows) = hot.floats.get(tenant) {
            for row in rows.iter() {
                if !row_matches(row.fp, &row.labels, row.ts_ms, &prepared, start_ms, end_ms) {
                    continue;
                }
                upper_count += 1;
                if upper_count > max_samples {
                    return Ok(None);
                }
                let sample = (row.fp, row.ts_ms, row.value, row.start_timestamp_ms);
                latest
                    .entry(row.fp)
                    .and_modify(|previous| {
                        // The full merge keeps the first hot row at an equal
                        // timestamp. A newer stale marker must remain selected.
                        if row.ts_ms > previous.1 {
                            *previous = sample;
                        }
                    })
                    .or_insert(sample);
            }
        }
        let index = self.cold.floats.index();
        let fps = index
            .resolve(tenant, matchers)
            .map_err(|error| PromqlError::Store(error.to_string()))?;
        let keys = index.candidate_blocks(tenant, &fps, start_ms, end_ms);
        let mut covered_blocks = 0;
        for block in index.all_blocks(tenant) {
            if !keys.contains(&block.object_key) {
                continue;
            }
            covered_blocks += 1;
            upper_count = upper_count.saturating_add(block.row_count);
            if upper_count > max_samples
                || block.fingerprints.is_empty()
                || block
                    .fingerprints
                    .iter()
                    .filter(|fp| fps.contains(fp))
                    .any(|fp| latest.get(fp).is_none_or(|row| row.1 < block.max_ts))
            {
                return Ok(None);
            }
        }
        if covered_blocks != keys.len() {
            return Ok(None);
        }
        let mut rows = latest.into_values().collect::<Vec<_>>();
        rows.sort_unstable_by_key(|row| row.0);
        Ok(Some(rows))
    }
}
