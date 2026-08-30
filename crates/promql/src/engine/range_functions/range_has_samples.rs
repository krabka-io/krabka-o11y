use super::*;

pub(crate) fn range_has_samples(series: &RangeSeries, range_end_ms: i64, range: Time) -> bool {
    range_sample_count(series, range_end_ms, range) != 0
}
