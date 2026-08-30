use super::DecodedSeries;

pub(crate) fn sample_timestamp_bounds(series: &DecodedSeries) -> Option<(i64, i64)> {
    series
        .samples
        .iter()
        .map(|sample| sample.timestamp_ms)
        .chain(
            series
                .histograms
                .iter()
                .map(|(timestamp_ms, _)| *timestamp_ms),
        )
        .chain(
            series
                .exemplars
                .iter()
                .map(|exemplar| exemplar.timestamp_ms),
        )
        .fold(None, |bounds, timestamp| match bounds {
            None => Some((timestamp, timestamp)),
            Some((min_timestamp, max_timestamp)) => {
                Some((min_timestamp.min(timestamp), max_timestamp.max(timestamp)))
            }
        })
}
