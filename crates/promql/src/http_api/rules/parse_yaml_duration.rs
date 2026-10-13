use super::{Time, TimeExt};

struct DurationParts<'a> {
    amount: &'a str,
    unit_seconds: i64,
}

/// Parses a duration with an `s`, `m`, or `h` suffix, the suffixes this surface
/// accepts. A bare number is a count of seconds. The amount parses as `u64`, so
/// this function returns `None` for a negative or otherwise malformed value.
/// The caller reads `None` as "no duration", not as a backwards window.
pub(crate) fn parse_yaml_duration(value: &str) -> Option<Time> {
    let value = value.trim();
    let parts = if let Some(amount) = value.strip_suffix('s') {
        DurationParts {
            amount,
            unit_seconds: 1,
        }
    } else if let Some(amount) = value.strip_suffix('m') {
        DurationParts {
            amount,
            unit_seconds: 60,
        }
    } else if let Some(amount) = value.strip_suffix('h') {
        DurationParts {
            amount,
            unit_seconds: 60 * 60,
        }
    } else {
        DurationParts {
            amount: value,
            unit_seconds: 1,
        }
    };
    let amount = i64::try_from(parts.amount.parse::<u64>().ok()?).ok()?;
    Some(Time::from_secs(amount.saturating_mul(parts.unit_seconds)))
}
