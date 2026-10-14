use super::DecodedSeries;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SampleTimestampBounds {
    pub min_ms: i64,
    pub max_ms: i64,
}

pub(crate) fn sample_timestamp_bounds(series: &DecodedSeries) -> Option<SampleTimestampBounds> {
    series
        .samples
        .iter()
        .map(|sample| sample.timestamp_ms)
        .chain(series.histograms.iter().map(|sample| sample.timestamp_ms))
        .chain(
            series
                .exemplars
                .iter()
                .map(|exemplar| exemplar.timestamp_ms),
        )
        .fold(None, |bounds, timestamp| match bounds {
            None => Some(SampleTimestampBounds {
                min_ms: timestamp,
                max_ms: timestamp,
            }),
            Some(bounds) => Some(SampleTimestampBounds {
                min_ms: bounds.min_ms.min(timestamp),
                max_ms: bounds.max_ms.max(timestamp),
            }),
        })
}
