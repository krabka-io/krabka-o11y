use super::{
    ApiError, BTreeMap, LabelMatcher, Labels, MetricStore, SeriesFingerprint, pb,
    remote_read_exemplar, remote_read_series,
};

pub(crate) async fn append_remote_read_exemplars<S: MetricStore>(
    store: &S,
    tenant: &str,
    matchers: &[LabelMatcher],
    start_ms: i64,
    end_ms: i64,
    labels_by_fp: &mut BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
) -> Result<(), ApiError> {
    for exemplar in store
        .exemplars(tenant, matchers, start_ms, end_ms)
        .await
        .map_err(ApiError::from)?
    {
        let fp = exemplar.series_labels.fingerprint();
        labels_by_fp
            .entry(fp)
            .or_insert_with(|| exemplar.series_labels.clone());
        let series = remote_read_series(by_fp, labels_by_fp, fp)?;
        series.exemplars.push(remote_read_exemplar(&exemplar));
    }
    Ok(())
}
