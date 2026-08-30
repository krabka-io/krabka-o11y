use super::*;

/// `validate_loki_volume_query_range_limit` caps a volume query's span at
/// 30 days and a bit. The cap is exclusive of nothing -- a range exactly at
/// the limit is allowed and one nanosecond more is not, which is the pair
/// separating `>` from `>=`.
///
/// A span that overflows an i64 subtraction is refused too, and reports the
/// widest length rather than a negative one: a wrapped subtraction would
/// otherwise report a query "shorter" than the limit and let it through.
#[test]
pub(crate) fn a_volume_query_range_is_capped_at_its_limit_exactly() {
    use krabka_blockstore::TimeRange;

    let max_ns = super::super::prelude::LOKI_VOLUME_MAX_QUERY_RANGE.nanos_i64();
    let range = |start_ns, end_ns| {
        super::super::prelude::validate_loki_volume_query_range_limit(
            TimeRange::new(start_ns, end_ns).expect("a valid range"),
        )
    };

    check!(range(0, 0).is_ok(), "an empty range is within any limit");
    check!(range(0, max_ns).is_ok(), "exactly at the limit");
    check!(range(1_000, 1_000 + max_ns).is_ok(), "wherever it starts");
    check!(range(0, max_ns + 1).is_err(), "one nanosecond over");

    // The error names how long the query actually was, so the client can
    // see by how much it missed.
    let error = range(0, max_ns + 1).expect_err("over the limit");
    check!(matches!(
        error,
        HttpQueryError::LokiQueryRangeTooLarge { .. }
    ));

    // A span that cannot be subtracted without overflowing is refused
    // rather than wrapping to a small positive number.
    check!(range(i64::MIN, i64::MAX).is_err(), "an overflowing span");
}
