use super::{ApiError, BTreeMap, Labels, MetricStore, PrometheusApiState, PromqlError, ScanResult, SeriesFingerprint, decode_native_histograms, enforce_sample_count, pb, remote_read_histogram, remote_read_series};

pub(crate) async fn append_remote_read_histogram_samples<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    scan: &ScanResult,
    table: &str,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    returned_samples: &mut u64,
) -> Result<(), ApiError> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT * FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;
    let batches = dataframe
        .collect()
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;

    for batch in batches {
        for (fp, timestamp, hist) in decode_native_histograms(&batch)
            .map_err(|error| ApiError::internal(error.to_string()))?
        {
            *returned_samples = returned_samples.saturating_add(1);
            enforce_sample_count(state, tenant, *returned_samples)?;
            let series = remote_read_series(by_fp, labels_by_fp, fp)?;
            series
                .histograms
                .push(remote_read_histogram(timestamp, &hist));
        }
    }
    Ok(())
}
