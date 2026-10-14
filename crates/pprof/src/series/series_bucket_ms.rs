use krabka_units::convert::TimeExt as _;

use super::Time;
use crate::MillisRange;

/// Returns a query-start-anchored right endpoint for a profile timestamp.
///
/// The first bucket includes the preceding step. Timestamps outside that
/// lookback or whose endpoint exceeds the query end have no output bucket.
/// Invalid ranges and steps shorter than one millisecond also return `None`.
#[must_use]
pub fn series_bucket_ms(timestamp: i64, step: Time, range: MillisRange) -> Option<i64> {
    let step = i128::from(step.millis_i64());
    let start = i128::from(range.start_ms);
    let end = i128::from(range.end_ms);
    let timestamp = i128::from(timestamp);
    if step <= 0 || end < start || timestamp < start - step || timestamp > end {
        return None;
    }
    let delta = (timestamp - start).max(0);
    let endpoint = start + (delta + step - 1) / step * step;
    (endpoint <= end)
        .then(|| i64::try_from(endpoint).ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use krabka_units::millis;

    use crate::MillisRange;

    #[yare::parameterized(
        just_before_lookback = { 10, None },
        start_of_lookback = { 11, Some(1011) },
        query_start = { 1011, Some(1011) },
        first_step = { 1012, Some(2011) },
        exact_step = { 2011, Some(2011) },
        second_step = { 2012, Some(3011) },
        exact_second_step = { 3011, Some(3011) },
        partial_final_bucket = { 3012, None },
        query_end = { 3511, None },
        after_query_end = { 3512, None },
    )]
    fn right_endpoints_keep_lookback_boundaries_and_clip_partial_last_bucket(
        timestamp: i64,
        expected: Option<i64>,
    ) {
        assert2::assert!(
            crate::series_bucket_ms(timestamp, millis(1000), MillisRange::new(1011, 3511))
                == expected
        );
    }

    #[test]
    fn extreme_ranges_preserve_checked_endpoint_behavior() {
        assert2::assert!(
            super::series_bucket_ms(i64::MAX, millis(10), MillisRange::new(i64::MAX, i64::MAX),)
                == Some(i64::MAX)
        );
        assert2::assert!(
            super::series_bucket_ms(i64::MIN, millis(10), MillisRange::new(i64::MIN, i64::MIN),)
                == Some(i64::MIN)
        );
        assert2::assert!(super::series_bucket_ms(1, millis(0), MillisRange::new(0, 2)).is_none());
        assert2::assert!(super::series_bucket_ms(1, millis(1), MillisRange::new(2, 0)).is_none());
    }
}
