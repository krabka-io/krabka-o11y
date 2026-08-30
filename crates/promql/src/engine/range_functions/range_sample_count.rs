use super::*;

pub(crate) fn range_sample_count(series: &RangeSeries, range_end_ms: i64, range: Time) -> usize {
    range_samples(series, range_end_ms, range).count()
}
