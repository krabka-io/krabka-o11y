use super::*;

/// `format_loki_offset_duration_ns` spells a duration the way `Loki` does,
/// picking the largest unit that fits. Each `>=` is the boundary between
/// two units, so each is checked exactly at its own threshold and one
/// step below it -- a `<` there sends the value to the next unit down.
#[test]
pub(crate) fn a_loki_offset_duration_picks_the_largest_unit_that_fits() {
    let format = super::super::prelude::format_loki_offset_duration_ns;

    // Zero is a duration, not an absence. A `<= 0` guard would lose it.
    check!(format(0) == Some("0s".to_string()));
    // Negative is an absence, which is what separates `< 0` from `== 0`.
    check!(format(-1).is_none());
    check!(format(-3_600_000_000_000).is_none());

    // At and just below the seconds boundary.
    check!(format(1_000_000_000) == Some("1s".to_string()));
    check!(format(1_500_000_000) == Some("1.5s".to_string()));
    check!(format(999_999_999) == Some("999.999999ms".to_string()));

    // At and just below the milliseconds boundary.
    check!(format(1_000_000) == Some("1ms".to_string()));
    check!(format(999_999) == Some("999.999\u{00b5}s".to_string()));

    // At and just below the microseconds boundary.
    check!(format(1_000) == Some("1\u{00b5}s".to_string()));
    check!(format(999) == Some("999ns".to_string()));

    // Larger units compose rather than replacing one another.
    check!(format(3_600_000_000_000) == Some("1h0m0s".to_string()));
    check!(format(90_000_000_000) == Some("1m30s".to_string()));
}
