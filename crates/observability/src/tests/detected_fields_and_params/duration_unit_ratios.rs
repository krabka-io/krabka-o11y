use super::*;

/// The parser's unit scale is a chain of powers and calendar units.
#[yare::parameterized(
    nanosecond_to_microsecond = { "ns", "us", 1_000_i128 },
    microsecond_to_millisecond = { "us", "ms", 1_000_i128 },
    millisecond_to_second = { "ms", "s", 1_000_i128 },
    second_to_minute = { "s", "m", 60_i128 },
    minute_to_hour = { "m", "h", 60_i128 },
    hour_to_day = { "h", "d", 24_i128 },
    day_to_week = { "d", "w", 7_i128 },
    day_to_year = { "d", "y", 365_i128 },
)]
fn duration_unit_ratios_match_expected_scale(smaller: &str, larger: &str, ratio: i128) {
    let nanoseconds = |name| {
        crate::tests::prelude::prometheus_duration_unit(name)
            .expect("known unit")
            .nanoseconds
    };
    check!(nanoseconds(larger) == ratio * nanoseconds(smaller));
}
