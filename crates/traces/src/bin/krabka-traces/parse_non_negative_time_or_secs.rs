use super::*;

pub(crate) fn parse_non_negative_time_or_secs(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_secs, false)
}
