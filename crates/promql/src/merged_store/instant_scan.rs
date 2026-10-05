use std::collections::HashMap;

use futures::TryStreamExt;
use krabka_blockstore::LabelMatcher;
use krabka_metrics::{FloatSampleRow, decode_float_samples, float_sample_schema};

use super::MergedMetricStore;
use crate::{
    MetricBlockStore, MetricStore, PromqlError, WalHead,
    in_memory::{prepare_matchers, row_matches},
};

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
        let mut uncovered = Vec::new();
        for block in index.all_blocks(tenant) {
            if !keys.contains(&block.object_key) {
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
                .filter(|fp| fps.contains(fp))
                .any(|fp| latest.get(fp).is_none_or(|row| row.1 < block.max_ts))
            {
                uncovered.push(block.object_key.clone());
            }
        }
        if covered_blocks != keys.len() || (!uncovered.is_empty() && latest.is_empty()) {
            return Ok(None);
        }
        if !uncovered.is_empty() {
            // A WAL-head reader can briefly trail a newly published block.
            // Read only blocks that can change a latest value, using the same
            // read caps, footer cache and streaming scan as the ordinary path.
            let scan = self
                .cold
                .floats
                .scan_block_keys_skipping_unreadable(&uncovered, float_sample_schema())
                .await
                .map_err(|error| PromqlError::Store(error.to_string()))?;
            if !scan.report.skipped.is_empty() {
                // The full scan retains the established missing-block warnings
                // and corrupt-block errors. Do not lose them in a row-only result.
                return Ok(None);
            }
            let mut stream = scan.ctx.table(&scan.table).await?.execute_stream().await?;
            let mut cold_latest = HashMap::<u64, FloatSampleRow>::new();
            while let Some(batch) = stream.try_next().await? {
                let Ok(rows) = decode_float_samples(&batch) else {
                    return Ok(None);
                };
                for row in rows {
                    if !fps.contains(&row.0) || row.1 < start_ms || row.1 > end_ms {
                        continue;
                    }
                    if let Some(previous) = cold_latest.get_mut(&row.0) {
                        if previous.1 == row.1
                            && (previous.2.to_bits() != row.2.to_bits() || previous.3 != row.3)
                        {
                            // Equal-time conflicting cold values retain the
                            // full merge's tie behavior instead of choosing anew.
                            return Ok(None);
                        }
                        if row.1 > previous.1 {
                            *previous = row;
                        }
                    } else {
                        cold_latest.insert(row.0, row);
                    }
                }
            }
            for (fp, row) in cold_latest {
                latest
                    .entry(fp)
                    .and_modify(|previous| {
                        // Hot wins at equal timestamps, just as in a full scan.
                        if row.1 > previous.1 {
                            *previous = row;
                        }
                    })
                    .or_insert(row);
            }
        }
        let mut rows = latest.into_values().collect::<Vec<_>>();
        rows.sort_unstable_by_key(|row| row.0);
        Ok(Some(rows))
    }
}
