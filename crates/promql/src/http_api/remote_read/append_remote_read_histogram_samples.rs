use super::{
    ApiError, MetricStore, RemoteReadSampleSink, ScanResult, collect_remote_read_batches,
    decode_native_histograms, remote_read_histogram,
};

pub(crate) async fn append_remote_read_histogram_samples<S: MetricStore>(
    sink: &mut RemoteReadSampleSink<'_, S>,
    scan: &ScanResult,
    table: &str,
) -> Result<(), ApiError> {
    let batches = collect_remote_read_batches(
        scan,
        &format!("SELECT * FROM {table} ORDER BY series_fingerprint, timestamp"),
    )
    .await?;

    for batch in batches {
        for row in decode_native_histograms(&batch)
            .map_err(|error| ApiError::internal(error.to_string()))?
        {
            let series = sink.next_sample_series(row.fingerprint)?;
            series
                .histograms
                .push(remote_read_histogram(row.timestamp_ms, &row.hist));
        }
    }
    Ok(())
}
