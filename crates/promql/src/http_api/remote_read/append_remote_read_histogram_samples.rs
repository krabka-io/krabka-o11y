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
        for (fp, timestamp, hist) in decode_native_histograms(&batch)
            .map_err(|error| ApiError::internal(error.to_string()))?
        {
            let series = sink.next_sample_series(fp)?;
            series
                .histograms
                .push(remote_read_histogram(timestamp, &hist));
        }
    }
    Ok(())
}
