use krabka_units::convert::TimeExt as _;

use super::Time;

/// Returns a query-start-anchored right endpoint for a profile timestamp.
///
/// The first bucket includes the preceding step. Timestamps outside that
/// lookback or whose endpoint exceeds the query end have no output bucket.
/// Invalid ranges and steps shorter than one millisecond also return `None`.
#[must_use]
pub fn series_bucket_ms(timestamp: i64, step: Time, range: (i64, i64)) -> Option<i64> {
    let step = i128::from(step.millis_i64());
    let (start, end) = (i128::from(range.0), i128::from(range.1));
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

    #[test]
    fn right_endpoints_keep_lookback_boundaries_and_clip_partial_last_bucket() {
        for (timestamp, expected) in [
            (10, None),
            (11, Some(1011)),
            (1011, Some(1011)),
            (1012, Some(2011)),
            (2011, Some(2011)),
            (2012, Some(3011)),
            (3011, Some(3011)),
            (3012, None),
            (3511, None),
            (3512, None),
        ] {
            assert2::assert!(
                super::series_bucket_ms(timestamp, millis(1000), (1011, 3511)) == expected
            );
        }
        assert2::assert!(
            super::series_bucket_ms(i64::MAX, millis(10), (i64::MAX, i64::MAX)) == Some(i64::MAX)
        );
        assert2::assert!(
            super::series_bucket_ms(i64::MIN, millis(10), (i64::MIN, i64::MIN)) == Some(i64::MIN)
        );
        assert2::assert!(super::series_bucket_ms(1, millis(0), (0, 2)).is_none());
        assert2::assert!(super::series_bucket_ms(1, millis(1), (2, 0)).is_none());
    }
}
