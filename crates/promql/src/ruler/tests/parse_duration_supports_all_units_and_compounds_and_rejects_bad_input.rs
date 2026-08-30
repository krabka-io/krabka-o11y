use super::*;

#[test]
pub(crate) fn parse_duration_supports_all_units_and_compounds_and_rejects_bad_input() {
    // Compound multi-unit durations, single-unit coverage across the full
    // Prometheus unit set, and hard errors (`None`, never a zero extent) for
    // negative, empty, and unparseable input.
    for (input, want) in [
        ("1h30m", Some(millis(5_400_000))),
        ("100ms", Some(millis(100))),
        ("5s", Some(secs(5))),
        ("1w", Some(days(7))),
        ("1y", Some(days(365))),
        ("0", Some(Time::ZERO)),
        ("-5m", None),
        ("", None),
        ("5x", None),
        ("abc", None),
    ] {
        assert2::assert!(super::parse_duration(input).ok() == want);
    }
}
