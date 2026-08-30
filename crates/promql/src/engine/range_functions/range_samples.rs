use super::{RangeSeries, Time, SampleValue, TimeExt};

pub(crate) fn range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> impl Iterator<Item = (i64, &SampleValue)> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    series
        .samples
        .iter()
        // Both comparisons here are permanent mutation survivors, and so is the
        // `&&`. Every `RangeEval` arrives from a matrix selector or a subquery
        // that has already fetched exactly `(end - range, end]`, so this
        // re-applies a window that is always a no-op -- removing the filter
        // outright leaves the whole suite, conformance corpus included, green.
        // It stays as the function's stated contract: four construction sites
        // feed `RangeEval`, and a fifth should not be able to widen a window
        // by forgetting to trim.
        .filter(move |(timestamp, _)| *timestamp > range_start_ms && *timestamp <= range_end_ms)
        .map(|(timestamp, value)| (*timestamp, value))
}
