use super::{
    ApiError, AsArray, Float64Type, Int64Type, MetricStore, RemoteReadSampleSink, ScanResult,
    UInt64Type, collect_remote_read_batches, pb,
};

pub(crate) async fn append_remote_read_float_samples<S: MetricStore>(
    sink: &mut RemoteReadSampleSink<'_, S>,
    scan: &ScanResult,
    table: &str,
) -> Result<(), ApiError> {
    let batches = collect_remote_read_batches(
        scan,
        &format!(
            "SELECT series_fingerprint, timestamp, value FROM {table} ORDER BY series_fingerprint, timestamp"
        ),
    )
    .await?;

    for batch in batches {
        let fps = batch.column(0).as_primitive::<UInt64Type>();
        let timestamps = batch.column(1).as_primitive::<Int64Type>();
        let values = batch.column(2).as_primitive::<Float64Type>();
        for row in 0..batch.num_rows() {
            let series = sink.next_sample_series(fps.value(row))?;
            series.samples.push(pb::v1::Sample {
                timestamp: timestamps.value(row),
                value: values.value(row),
            });
        }
    }
    Ok(())
}
