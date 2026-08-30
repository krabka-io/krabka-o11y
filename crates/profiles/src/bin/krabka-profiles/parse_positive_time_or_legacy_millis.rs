use super::*;

pub(crate) fn parse_positive_time_or_legacy_millis(value: &str) -> Result<Time, String> {
    parse_positive_time_or_legacy(value, Time::from_millis)
}
