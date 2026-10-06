use std::{collections::BTreeMap, sync::Arc};

use super::{
    ExemplarScan, LabelMatcher, LabelNameCardinality, LabelValueCardinality, Labels,
    LatestFloatScan, MetadataScan, PromqlError, ScanResult, TsdbBlock, TsdbStats,
};

/// Resolves `PromQL` matchers to `DataFusion` tables over the metric data of a tenant.
#[async_trait::async_trait]
pub trait MetricStore: Send + Sync {
    /// Latest float sample per series for one instant, or `None` to use a full scan.
    ///
    /// An implementation must prove that its complete scan would stay within
    /// `max_samples`, preserve hot-source precedence and stale markers, and
    /// return at most one row per fingerprint. Bounds are inclusive. Range
    /// queries do not use this shortcut.
    async fn try_latest_float_samples(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
        _max_samples: usize,
    ) -> Result<Option<Vec<krabka_metrics::FloatSampleRow>>, PromqlError> {
        Ok(None)
    }

    /// Latest instant samples and complete labels from their respective windows.
    ///
    /// Both windows end at `end_ms`. Labels begin at `label_start_ms`, which
    /// must not exceed `sample_start_ms`. Labels include series without a
    /// selected latest sample so that the engine can enforce its series limit.
    /// The default composes the existing sample and shared-label operations.
    async fn try_latest_float_scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_start_ms: i64,
        sample_start_ms: i64,
        end_ms: i64,
        max_samples: usize,
    ) -> Result<Option<LatestFloatScan>, PromqlError> {
        let Some(samples) = self
            .try_latest_float_samples(tenant, matchers, sample_start_ms, end_ms, max_samples)
            .await?
        else {
            return Ok(None);
        };
        // Preserve the engine's sample-limit error before resolving labels.
        let labels = if samples.len() > max_samples {
            BTreeMap::default()
        } else {
            self.series_shared(tenant, matchers, label_start_ms, end_ms)
                .await?
                .into_iter()
                .map(|labels| (labels.fingerprint(), labels))
                .collect()
        };
        Ok(Some(LatestFloatScan { samples, labels }))
    }

    /// Registers the float and histogram tables for matched series in `[start_ms, end_ms]`.
    ///
    /// The result also names every block the scan answered without. See
    /// [`ScanResult::warnings`].
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, PromqlError>;

    /// Returns whether a scan of the matched series in `[start_ms, end_ms]` can return a histogram table.
    ///
    /// The engine asks this before a scan that looks only for histogram samples, and does not scan when the answer is `false`. A store answers `false` only when [`Self::scan`] with the same arguments returns no histogram table. The default answer is `true`, which is correct for every store.
    async fn may_have_histograms(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<bool, PromqlError> {
        Ok(true)
    }

    /// Returns the distinct label names across matched series.
    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError>;

    /// Returns the distinct values of `name` across matched series.
    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError>;

    /// Returns the label sets of matched series.
    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError>;

    /// Immutable labels of the same matched series as [`Self::series`].
    ///
    /// Stores with shared labels can retain them through query evaluation.
    /// The default preserves the owned-series behavior for other stores.
    async fn series_shared(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Arc<Labels>>, PromqlError> {
        Ok(self
            .series(tenant, matchers, start_ms, end_ms)
            .await?
            .into_iter()
            .map(Arc::new)
            .collect())
    }

    /// Returns the exemplars attached to matched series in `[start_ms, end_ms]`.
    ///
    /// The result also names every block the scan answered without. See
    /// [`ExemplarScan::warnings`].
    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ExemplarScan, PromqlError>;

    /// Returns the metric metadata for a tenant.
    ///
    /// The caller can restrict the result to one metric family. The result
    /// also names every block the scan answered without. See
    /// [`MetadataScan::warnings`].
    async fn metadata(
        &self,
        tenant: &str,
        metric: Option<&str>,
    ) -> Result<MetadataScan, PromqlError>;

    /// Returns the distinct active-series count for each label name in a tenant.
    async fn cardinality_label_names(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelNameCardinality>, PromqlError>;

    /// Returns the distinct active-series count for each label value in a tenant.
    async fn cardinality_label_values(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelValueCardinality>, PromqlError>;

    /// Returns the distinct label sets of the active series in a tenant.
    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>, PromqlError>;

    /// Returns the tenant-scoped TSDB status statistics.
    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats, PromqlError>;

    /// Returns the tenant-scoped metadata of the compacted blocks.
    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>, PromqlError>;
}
