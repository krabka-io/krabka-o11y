use super::*;

pub(crate) fn parse_positive_time_or_millis(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_millis, true)
}
