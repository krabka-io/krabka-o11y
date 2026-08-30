use super::{MAX_SAMPLE_TIMESTAMP_MS, OtlpError, metric};

/// Rejects any data point whose `time_unix_nano` is beyond the sane future
/// bound. A clamp of such a value to `i64::MAX` would poison the per-series
/// out-of-order and too-old window downstream, so this function drops the
/// request instead. Prometheus rejects a sample too far in the future the same
/// way.
pub(crate) fn reject_far_future_points(name: &str, data: &metric::Data) -> Result<(), OtlpError> {
    let mut timestamps = Vec::new();
    match data {
        metric::Data::Gauge(gauge) => {
            timestamps.extend(gauge.data_points.iter().map(|point| point.time_unix_nano));
        }
        metric::Data::Sum(sum) => {
            timestamps.extend(sum.data_points.iter().map(|point| point.time_unix_nano));
        }
        metric::Data::Histogram(histogram) => {
            timestamps.extend(
                histogram
                    .data_points
                    .iter()
                    .map(|point| point.time_unix_nano),
            );
        }
        metric::Data::ExponentialHistogram(histogram) => {
            timestamps.extend(
                histogram
                    .data_points
                    .iter()
                    .map(|point| point.time_unix_nano),
            );
        }
        metric::Data::Summary(summary) => {
            timestamps.extend(summary.data_points.iter().map(|point| point.time_unix_nano));
        }
    }
    if let Some(time_unix_nano) = timestamps
        .into_iter()
        .find(|time_unix_nano| time_unix_nano / 1_000_000 > MAX_SAMPLE_TIMESTAMP_MS)
    {
        return Err(OtlpError::Invalid(
            name.into(),
            format!("data point timestamp {time_unix_nano}ns is too far in the future"),
        ));
    }
    Ok(())
}
