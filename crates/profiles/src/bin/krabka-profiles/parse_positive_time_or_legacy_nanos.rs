use super::*;

pub(crate) fn parse_positive_time_or_legacy_nanos(value: &str) -> Result<Time, String> {
    parse_positive_time_or_legacy(value, Time::from_nanos)
}
