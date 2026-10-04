use super::{
    ExemplarScan, LabelMatcher, LabelNameCardinality, LabelValueCardinality, Labels, MetadataScan,
    PromqlError, ScanResult, TsdbBlock, TsdbStats,
};

/// Resolves `PromQL` matchers to `DataFusion` tables over the metric data of a tenant.
#[async_trait::async_trait]
pub trait MetricStore: Send + Sync {
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
