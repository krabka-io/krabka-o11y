use super::*;

/// `format_loki_query_length` always writes all three units, including the
/// zero ones -- "0h5m0s" rather than "5m". That is the opposite of
/// `format_loki_duration_ns`, which skips empty units, and the two are
/// pinned separately because the difference is deliberate: this one is a
/// fixed-shape field a client parses positionally.
#[test]
pub(crate) fn a_loki_query_length_always_writes_all_three_units() {
    let format =
        |seconds: i64| super::super::prelude::format_loki_query_length(Time::from_nanos(seconds));
    let secs = 1_000_000_000_i64;

    check!(format(0) == "0h0m0s", "every unit, even at zero");
    check!(format(5 * secs) == "0h0m5s");
    check!(format(300 * secs) == "0h5m0s", "zero seconds still written");
    check!(format(3_600 * secs) == "1h0m0s");
    check!(format(3_661 * secs) == "1h1m1s");
    check!(format(7_322 * secs) == "2h2m2s");

    // Hours accumulate rather than rolling into a larger unit.
    check!(format(100 * 3_600 * secs) == "100h0m0s");

    // Sub-second precision is dropped, not rounded up.
    check!(format(secs - 1) == "0h0m0s");

    // A negative range is clamped to zero rather than writing minus signs
    // into a field a client parses positionally.
    check!(format(-secs) == "0h0m0s");
}
