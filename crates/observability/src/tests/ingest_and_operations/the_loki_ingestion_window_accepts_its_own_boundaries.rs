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
        super::super::prelude::validate_loki_timestamp_window_at(
            timestamp, now, &labels, max_age, grace,
        )
    };
    let hour_ns = hours(1).nanos_i64();

    // Exactly at the oldest acceptable timestamp: accepted. One
    // nanosecond older: refused.
    check!(check_at(now - hour_ns, hours(1), Time::ZERO).is_ok());
    check!(check_at(now - hour_ns + 1, hours(1), Time::ZERO).is_ok());
    check!(check_at(now - hour_ns - 1, hours(1), Time::ZERO).is_err());

    // Exactly at the newest acceptable timestamp: accepted. One
    // nanosecond newer: refused.
    check!(check_at(now + hour_ns, Time::ZERO, hours(1)).is_ok());
    check!(check_at(now + hour_ns - 1, Time::ZERO, hours(1)).is_ok());
    check!(check_at(now + hour_ns + 1, Time::ZERO, hours(1)).is_err());

    // A bound of zero imposes nothing, and the two are independent: an
    // ancient timestamp passes with no max age, and a far-future one
    // passes with no grace period.
    check!(check_at(0, Time::ZERO, hours(1)).is_ok());
    check!(check_at(i64::MAX / 2, hours(1), Time::ZERO).is_ok());
    check!(check_at(0, Time::ZERO, Time::ZERO).is_ok());
    check!(check_at(i64::MAX, Time::ZERO, Time::ZERO).is_ok());

    // A one-nanosecond window admits the instant itself and nothing else.
    // Zero is the sentinel for "no window", so the tightest real window
    // that can be expressed is a nanosecond wide.
    check!(check_at(now, nanos(1), nanos(1)).is_ok());
    check!(check_at(now - 2, nanos(1), Time::ZERO).is_err());
    check!(check_at(now + 2, Time::ZERO, nanos(1)).is_err());

    // The refusals name their own direction rather than sharing one error.
    check!(matches!(
        check_at(now - hour_ns - 1, hours(1), Time::ZERO),
        Err(DistributorError::TimestampTooOld { .. })
    ));
    check!(matches!(
        check_at(now + hour_ns + 1, Time::ZERO, hours(1)),
        Err(DistributorError::TimestampTooNew { .. })
    ));
}
