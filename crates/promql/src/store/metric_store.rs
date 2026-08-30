use super::*;

/// Resolves `PromQL` matchers to `DataFusion` tables over the metric data of a tenant.
#[async_trait::async_trait]
pub trait MetricStore: Send + Sync {
    /// Registers the float and histogram tables for matched series in `[start_ms, end_ms]`.
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, PromqlError>;

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
    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>, PromqlError>;

    /// Returns the metric metadata for a tenant.
    ///
    /// The caller can restrict the result to one metric family.
    async fn metadata(
        &self,
        tenant: &str,
        metric: Option<&str>,
    ) -> Result<Vec<MetadataRecord>, PromqlError>;

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
