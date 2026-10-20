use super::{DecodedSeries, HistogramFamily, HistogramScope, OtlpError};

pub(super) fn decode_histogram_points<T>(
    scope: &HistogramScope<'_>,
    points: &[T],
    mut decode: impl FnMut(&T, &HistogramFamily) -> Result<Vec<DecodedSeries>, OtlpError>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let family = HistogramFamily::of(scope.metric, scope.strategy);
    let mut series = Vec::new();
    for point in points {
        series.extend(decode(point, &family)?);
    }
    Ok(series)
}
