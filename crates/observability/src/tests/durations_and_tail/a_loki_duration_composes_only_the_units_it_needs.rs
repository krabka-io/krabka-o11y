use super::*;

/// `format_loki_duration_ns` composes a duration from the largest unit
/// down, SKIPPING units that contribute nothing -- so 3661s is "1h1m1s"
/// and not "1h1m1s0ms0us0ns". Zero is the one duration spelled with a unit
/// it does not contain, because "" would not read as a duration at all.
#[test]
pub(crate) fn a_loki_duration_composes_only_the_units_it_needs() {
    let format = super::super::prelude::format_loki_duration_ns;

    // Each unit alone.
    check!(format(3_600_000_000_000) == Some("1h".to_string()));
    check!(format(60_000_000_000) == Some("1m".to_string()));
    check!(format(1_000_000_000) == Some("1s".to_string()));
    check!(format(1_000_000) == Some("1ms".to_string()));
    check!(format(1_000) == Some("1us".to_string()));
    check!(format(1) == Some("1ns".to_string()));

    // Composed, with the gaps left out rather than written as zeros.
    check!(format(3_661_000_000_000) == Some("1h1m1s".to_string()));
    check!(
        format(3_600_000_000_001) == Some("1h1ns".to_string()),
        "no zero units between"
    );
    check!(format(90_000_000_000) == Some("1m30s".to_string()));
    check!(format(1_500_000) == Some("1ms500us".to_string()));

    // Counts above one, and a unit that repeats rather than rolling over
    // into the next -- 90 minutes is an hour and a half, not "90m".
    check!(format(2 * 3_600_000_000_000) == Some("2h".to_string()));
    check!(format(90 * 60_000_000_000) == Some("1h30m".to_string()));

    // Zero and negative are different answers: a zero duration is a
    // duration, a negative one is not.
    check!(format(0) == Some("0s".to_string()));
    check!(format(-1).is_none());
    check!(format(-3_600_000_000_000).is_none());
}
