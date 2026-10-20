//! Parsers shared by the profiles command-line options.

use krabka_units::convert::TimeExt as _;

use super::{Time, parse};

pub(crate) fn parse_min_two_usize(value: &str) -> Result<usize, String> {
    use refined_type::rule::GreaterUsize;

    GreaterUsize::<1>::new(value.parse::<usize>().map_err(|error| error.to_string())?)
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}

pub(crate) fn parse_non_empty_string(value: &str) -> Result<String, String> {
    refined_type::rule::NonEmptyString::new(value.to_owned())
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}

pub(crate) fn parse_positive_time_or_legacy(
    value: &str,
    legacy: fn(i64) -> Time,
) -> Result<Time, String> {
    if let Ok(raw) = value.parse::<i64>() {
        if raw <= 0 {
            return Err("time must be positive".to_owned());
        }
        return Ok(legacy(raw));
    }
    parse::positive_time(value).map_err(|error| error.to_string())
}

pub(crate) fn parse_positive_time_or_legacy_millis(value: &str) -> Result<Time, String> {
    parse_positive_time_or_legacy(value, Time::from_millis)
}

pub(crate) fn parse_positive_time_or_legacy_nanos(value: &str) -> Result<Time, String> {
    parse_positive_time_or_legacy(value, Time::from_nanos)
}
