use super::*;

/// Parses a duration with an `s`, `m`, or `h` suffix, the suffixes this surface
/// accepts. A bare number is a count of seconds. The amount parses as `u64`, so
/// this function returns `None` for a negative or otherwise malformed value.
/// The caller reads `None` as "no duration", not as a backwards window.
pub(crate) fn parse_yaml_duration(value: &str) -> Option<Time> {
    let value = value.trim();
    let (amount, unit_seconds) = if let Some(amount) = value.strip_suffix('s') {
        (amount, 1)
    } else if let Some(amount) = value.strip_suffix('m') {
        (amount, 60)
    } else if let Some(amount) = value.strip_suffix('h') {
        (amount, 60 * 60)
    } else {
        (value, 1)
    };
    let amount = i64::try_from(amount.parse::<u64>().ok()?).ok()?;
    Some(Time::from_secs(amount.saturating_mul(unit_seconds)))
}
