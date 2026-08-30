use super::{MetricStore, PrometheusApiState, ScanResult, BTreeMap, SeriesFingerprint, Labels, pb, ApiError, PromqlError, AsArray, UInt64Type, Int64Type, Float64Type, enforce_sample_count, remote_read_series};

pub(crate) async fn append_remote_read_float_samples<S: MetricStore>(
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
            "SELECT series_fingerprint, timestamp, value FROM {table} ORDER BY series_fingerprint, timestamp"
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
        let fps = batch.column(0).as_primitive::<UInt64Type>();
        let timestamps = batch.column(1).as_primitive::<Int64Type>();
        let values = batch.column(2).as_primitive::<Float64Type>();
        for row in 0..batch.num_rows() {
            *returned_samples = returned_samples.saturating_add(1);
            enforce_sample_count(state, tenant, *returned_samples)?;
            let series = remote_read_series(by_fp, labels_by_fp, fps.value(row))?;
            series.samples.push(pb::v1::Sample {
                timestamp: timestamps.value(row),
                value: values.value(row),
            });
        }
    }
    Ok(())
}
