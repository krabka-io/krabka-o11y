use super::*;

pub(crate) fn parse_positive_time_or_secs(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_secs, true)
}
