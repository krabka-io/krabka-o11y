use super::*;

/// Both ends of the `Loki` ingestion window are strict comparisons: a
/// timestamp exactly at the oldest or the newest acceptable value is
/// accepted. That is the only input separating `<` from `<=`, and against
/// a wall clock it is unreachable -- `now` advances between choosing the
/// timestamp and the function reading it. Hence the `_at` seam, which
/// takes `now` rather than reading it.
#[test]
pub(crate) fn the_loki_ingestion_window_accepts_its_own_boundaries() {
    use krabka_units::{hours, nanos};

    let now = 1_000_000_000_000_i64;
    let labels = Labels::default();
    let check_at = |timestamp: i64, max_age, grace| {
        super::super::prelude::validate_loki_timestamp_window_at(timestamp, now, &labels, max_age, grace)
    };
    let hour_ns = hours(1).nanos_i64();

    // Exactly at the oldest acceptable timestamp: accepted. One
    // nanosecond older: refused.
    check!(check_at(now - hour_ns, Some(hours(1)), None).is_ok());
    check!(check_at(now - hour_ns + 1, Some(hours(1)), None).is_ok());
    check!(check_at(now - hour_ns - 1, Some(hours(1)), None).is_err());

    // Exactly at the newest acceptable timestamp: accepted. One
    // nanosecond newer: refused.
    check!(check_at(now + hour_ns, None, Some(hours(1))).is_ok());
    check!(check_at(now + hour_ns - 1, None, Some(hours(1))).is_ok());
    check!(check_at(now + hour_ns + 1, None, Some(hours(1))).is_err());

    // A bound that is absent imposes nothing, and the two are
    // independent: an ancient timestamp passes with no max age, and a
    // far-future one passes with no grace period.
    check!(check_at(0, None, Some(hours(1))).is_ok());
    check!(check_at(i64::MAX / 2, Some(hours(1)), None).is_ok());
    check!(check_at(0, None, None).is_ok());
    check!(check_at(i64::MAX, None, None).is_ok());

    // A zero window admits only the instant itself.
    check!(check_at(now, Some(nanos(0)), Some(nanos(0))).is_ok());
    check!(check_at(now - 1, Some(nanos(0)), None).is_err());
    check!(check_at(now + 1, None, Some(nanos(0))).is_err());

    // The refusals name their own direction rather than sharing one error.
    check!(matches!(
        check_at(now - hour_ns - 1, Some(hours(1)), None),
        Err(DistributorError::TimestampTooOld { .. })
    ));
    check!(matches!(
        check_at(now + hour_ns + 1, None, Some(hours(1))),
        Err(DistributorError::TimestampTooNew { .. })
    ));
}
