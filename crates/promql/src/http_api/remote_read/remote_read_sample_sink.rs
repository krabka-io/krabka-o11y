use arrow::array::RecordBatch;

use super::{
    ApiError, BTreeMap, Labels, MetricStore, PrometheusApiState, PromqlError, ScanResult,
    SeriesFingerprint, TenantId, enforce_sample_count, pb, remote_read_series,
};

/// Where one remote-read query's samples go, counted against the tenant's
/// sample limit.
pub(crate) struct RemoteReadSampleSink<'a, S: MetricStore> {
    pub(crate) state: &'a PrometheusApiState<S>,
    pub(crate) tenant: &'a TenantId,
    pub(crate) labels_by_fp: &'a BTreeMap<SeriesFingerprint, Labels>,
    pub(crate) series_by_fp: &'a mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    pub(crate) returned_samples: &'a mut u64,
}

impl<S: MetricStore> RemoteReadSampleSink<'_, S> {
    /// Counts one more returned sample and returns the series it belongs to.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError`] once the query exceeds the tenant's sample limit,
    /// or when the series has no labels.
    pub(crate) fn next_sample_series(
        &mut self,
        fp: SeriesFingerprint,
    ) -> Result<&mut pb::v1::TimeSeries, ApiError> {
        *self.returned_samples = self.returned_samples.saturating_add(1);
        enforce_sample_count(self.state, self.tenant, *self.returned_samples)?;
        remote_read_series(self.series_by_fp, self.labels_by_fp, fp)
    }
}

/// Runs `sql` against the scan's session and collects its batches.
pub(crate) async fn collect_remote_read_batches(
    scan: &ScanResult,
    sql: &str,
) -> Result<Vec<RecordBatch>, ApiError> {
    let dataframe = scan
        .ctx
        .sql(sql)
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;
    dataframe
        .collect()
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)
}
